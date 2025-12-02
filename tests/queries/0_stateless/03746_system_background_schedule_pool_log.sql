-- Test for MergeTree table (schedule pool)
DROP TABLE IF EXISTS test_merge_tree_03745;
CREATE TABLE test_merge_tree_03745 (x UInt64, y String) ENGINE = MergeTree() ORDER BY x;
INSERT INTO test_merge_tree_03745 VALUES (1, 'a'), (2, 'b');
SYSTEM FLUSH LOGS background_schedule_pool_log;
SELECT DISTINCT database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name, query_id != '' FROM system.background_schedule_pool_log WHERE database = currentDatabase() AND table = 'test_merge_tree_03745';
DROP TABLE test_merge_tree_03745;

-- Test for Distributed table (distributed pool)
DROP TABLE IF EXISTS test_local_03745;
DROP TABLE IF EXISTS test_distributed_03745;
CREATE TABLE test_local_03745 (x UInt64) ENGINE = Null;
CREATE TABLE test_distributed_03745 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_local_03745);
-- Pool is created only for async INSERTs
INSERT INTO test_distributed_03745 SELECT * FROM numbers(10e6) SETTINGS prefer_localhost_replica=0, distributed_foreground_insert=0;
SYSTEM FLUSH DISTRIBUTED test_distributed_03745;
SYSTEM FLUSH LOGS background_schedule_pool_log;
SELECT database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name, max(duration_ms) > 0, query_id != '' FROM system.background_schedule_pool_log WHERE database = currentDatabase() AND table = 'test_distributed_03745' GROUP BY ALL;
DROP TABLE test_distributed_03745;
DROP TABLE test_local_03745;
