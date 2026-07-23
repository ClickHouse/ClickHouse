-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: relies on per-replica `mutationsUpdatingTask` path

-- Test: exercises `ReplicatedMergeTreeQueue::updateMutations` early-abort branch added at
--       src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp:1134 — when SYSTEM STOP PULLING
--       REPLICATION LOG is active, `mutationsUpdatingTask` must NOT load mutation entries
--       into local state. Without the fix, the background `mutationsUpdatingTask` would still
--       pull mutations from ZK (the existing `pull_log_blocker.isCancelled()` guard at line 828
--       only protected `pullLogsToQueue`, not the direct `updateMutations` call from the
--       background task at `StorageReplicatedMergeTree.cpp:4073`).

DROP TABLE IF EXISTS r1 SYNC;
DROP TABLE IF EXISTS r2 SYNC;

CREATE TABLE r1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_pr59895/t', 'r1') ORDER BY tuple();
CREATE TABLE r2 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_pr59895/t', 'r2') ORDER BY tuple();

INSERT INTO r1 VALUES (1),(2),(3);
SYSTEM SYNC REPLICA r2;

-- Stop pulling on r2; from now on r2's `mutationsUpdatingTask` must early-abort.
SYSTEM STOP PULLING REPLICATION LOG r2;

-- Run mutation; mutations_sync=1 waits only for r1.
ALTER TABLE r1 UPDATE x = x + 100 WHERE 1 SETTINGS mutations_sync=1;

-- Give r2's background `mutationsUpdatingTask` time to fire on the ZK watch.
-- With the fix, the task throws ABORTED at line 1134 and reschedules; without the fix,
-- it would pull the mutation into r2's local state.
SELECT sleep(2) FORMAT Null;

-- r1 must see the mutation in local state.
SELECT 'r1_mutations', count() FROM system.mutations
  WHERE database = currentDatabase() AND table = 'r1';

-- r2 must NOT have loaded the mutation while pull is stopped.
SELECT 'r2_mutations_during_stop', count() FROM system.mutations
  WHERE database = currentDatabase() AND table = 'r2';

-- Restart pulling — mutation should now propagate.
SYSTEM START PULLING REPLICATION LOG r2;
SYSTEM SYNC REPLICA r2;

SELECT 'r2_mutations_after_start', count() FROM system.mutations
  WHERE database = currentDatabase() AND table = 'r2';

-- Confirm correctness of mutated data on r2 (1+100, 2+100, 3+100).
SELECT 'r2_data', x FROM r2 ORDER BY x;

DROP TABLE r1 SYNC;
DROP TABLE r2 SYNC;
