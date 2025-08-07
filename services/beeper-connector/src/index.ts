import { BeeperDbReader } from './beeperDbReader';
import { EvaultWriter } from './evaultWriter';
import { MetaStateTransformer } from './metaStateTransformer';
import { StateStore } from './state';

async function main() {
  console.log('Beeper Connector Service starting...');

  const beeperDbPath = process.env.BEEPER_DB_PATH;
  if (!beeperDbPath) {
    console.error('Error: BEEPER_DB_PATH environment variable is not set.');
    process.exit(1);
  }
  console.log(`Attempting to connect to Beeper DB at: ${beeperDbPath}`);

  const pollIntervalMs = Number(process.env.POLL_INTERVAL_MS ?? '0');
  const statePath = process.env.STATE_FILE_PATH;
  const stateStore = new StateStore(statePath);
  const state = stateStore.load();

  let dbReader: BeeperDbReader | null = null;

  try {
    dbReader = new BeeperDbReader(beeperDbPath);

    console.log('Fetching users...');
    const users = await dbReader.getUsers();
    console.log(`Found ${users.length} users:`, users.slice(0, 5));

      const firstUser = users[0];
      let threads: Awaited<ReturnType<typeof dbReader.getThreads>> = [];
      const messages: Awaited<ReturnType<typeof dbReader.getMessages>> = [];

      if (firstUser?.accountID) {
        const firstUserAccountId = firstUser.accountID;
        console.log(`Fetching threads for accountID: ${firstUserAccountId} ...`);
        threads = await dbReader.getThreads(firstUserAccountId);
        console.log(`Found ${threads.length} threads for account ${firstUserAccountId}:`, threads.slice(0, 3));
        for (const th of threads.slice(0, 5)) {
          const last = state.threads[th.threadID]?.lastTimestampMs;
          const sinceDate = last ? new Date(last) : undefined;
          const batch = await dbReader.getMessages(th.threadID, sinceDate, 50);
          messages.push(...batch);
        }
      } else {
        console.log('Skipping thread and message fetching as no users with an accountID found.');
      }

      // Transform Beeper records into MetaState envelopes
      const transformer = new MetaStateTransformer();
      const envelopes = transformer.transform({ users, threads, messages, sourcePlatform: 'Beeper' });

      // Write into eVault if configured
      const evaultEndpoint = process.env.EVAULT_ENDPOINT;
      const evaultAuthToken = process.env.EVAULT_AUTH_TOKEN;
      const aclEnv = process.env.W3ID_ACL; // JSON array or comma-separated
      const acl: string[] = (() => {
        if (!aclEnv) return [];
        try {
          const parsed = JSON.parse(aclEnv);
          return Array.isArray(parsed) ? parsed : [];
        } catch {
          return aclEnv.split(',').map((s) => s.trim()).filter(Boolean);
        }
      })();

      if (evaultEndpoint && envelopes.length > 0) {
        console.log(`Writing ${envelopes.length} envelopes to eVault at ${evaultEndpoint} ...`);
        const writer = new EvaultWriter(evaultEndpoint, evaultAuthToken);
        await writer.storeBatch(
          envelopes.map((payload) => ({ ontology: payload.ontology, payload: payload.payload, acl })),
          0
        );
        console.log('Write complete.');
        for (const m of messages) {
          const t = m.threadID;
          const ts = m.timestamp;
          const entry = state.threads[t] ?? { lastTimestampMs: 0 };
          if (!entry.lastTimestampMs || ts > entry.lastTimestampMs) {
            entry.lastTimestampMs = ts;
            state.threads[t] = entry;
          }
        }
        stateStore.save(state);
      } else if (!evaultEndpoint) {
        console.warn('EVAULT_ENDPOINT not set. Skipping write to eVault.');
      }

    console.log('Beeper Connector Service finished its run (data fetching test complete).');

  } catch (error) {
    console.error('Error in Beeper Connector Service:', error);
    process.exit(1);
  } finally {
    if (dbReader) {
      dbReader.close();
    }
  }
}

async function loop() {
  const interval = Number(process.env.POLL_INTERVAL_MS ?? '0');
  if (!interval || interval <= 0) {
    await main();
    return;
  }
  while (true) {
    try {
      await main();
    } catch (e) {
      console.error('Polling iteration failed:', e);
    }
    await new Promise((r) => setTimeout(r, interval));
  }
}

loop().catch(error => {
  console.error('Unhandled error in main loop:', error);
  process.exit(1);
});
