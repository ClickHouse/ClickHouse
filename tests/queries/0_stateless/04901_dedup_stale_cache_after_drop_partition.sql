-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree: StorageSharedMergeTree has its own sink.
-- no-replicated-database: extra replicas perturb the two-replica setup.

DROP TABLE IF EXISTS t_04901_r1 SYNC;
DROP TABLE IF EXISTS t_04901_r2 SYNC;

CREATE TABLE t_04901_r1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04901/t', 'r1') ORDER BY k;
CREATE TABLE t_04901_r2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04901/t', 'r2') ORDER BY k;

SET async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1;
SET async_insert_busy_timeout_min_ms = 10, async_insert_busy_timeout_max_ms = 20;

-- Registers the deduplication hash in Keeper.
INSERT INTO t_04901_r2 VALUES (1);
SYSTEM SYNC REPLICA t_04901_r1;

-- Deduplicated normally, which populates r2's deduplication hash cache.
INSERT INTO t_04901_r2 VALUES (1);

-- Removes the hash from Keeper; only r1 invalidates its own cache.
ALTER TABLE t_04901_r1 DROP PARTITION tuple() SETTINGS alter_sync = 2;
SYSTEM SYNC REPLICA t_04901_r2;

SELECT 'after drop', count() FROM t_04901_r2;

-- The prior copy was dropped, so this insert must land.
INSERT INTO t_04901_r2 VALUES (1);
SYSTEM SYNC REPLICA t_04901_r1;

SELECT 'after reinsert r2', count() FROM t_04901_r2;
SELECT 'after reinsert r1', count() FROM t_04901_r1;

-- The row counts above only test the bug if the last insert really was prefiltered by the stale
-- cache, and the cache refresh that arms it is asynchronous. Assert the hit happened on the insert
-- that landed (error = 0), so an earlier deduplicated insert cannot satisfy this on its behalf.
SYSTEM FLUSH LOGS part_log;
SELECT 'cache hit observed', sum(ProfileEvents['AsyncInsertCacheHits']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_04901_r2'
  AND event_type = 'NewPart' AND error = 0;

DROP TABLE t_04901_r1 SYNC;
DROP TABLE t_04901_r2 SYNC;
