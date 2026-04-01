SELECT count() >= 0 AS has_tasks FROM system.background_schedule_pool;

-- Test 1: Buffer table (buffer_flush pool)
DROP TABLE IF EXISTS test_table_03745;
DROP TABLE IF EXISTS test_buffer_03745;

CREATE TABLE test_table_03745 (x UInt64) ENGINE = Memory;
CREATE TABLE test_buffer_03745 (x UInt64) ENGINE = Buffer(currentDatabase(), test_table_03745, 1, 10, 100, 10000, 1000000, 10000000, 100000000);
INSERT INTO test_buffer_03745 VALUES (1), (2), (3);
SELECT pool, database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name FROM system.background_schedule_pool WHERE database = currentDatabase();
DROP TABLE test_buffer_03745;
DROP TABLE test_table_03745;

-- Test 2: MergeTree table (schedule pool)
DROP TABLE IF EXISTS test_merge_tree_03745;
CREATE TABLE test_merge_tree_03745 (x UInt64, y String) ENGINE = MergeTree() ORDER BY x;
INSERT INTO test_merge_tree_03745 VALUES (1, 'a'), (2, 'b');
SELECT pool, database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name FROM system.background_schedule_pool WHERE database = currentDatabase();
DROP TABLE test_merge_tree_03745;

-- Test 3: Distributed table (distributed pool)
DROP TABLE IF EXISTS test_local_03745;
DROP TABLE IF EXISTS test_distributed_03745;
CREATE TABLE test_local_03745 (x UInt64) ENGINE = Memory;
CREATE TABLE test_distributed_03745 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_local_03745);
SYSTEM STOP DISTRIBUTED SENDS test_distributed_03745;
-- Pool is created only for async INSERTs
INSERT INTO test_distributed_03745 SETTINGS prefer_localhost_replica=0, distributed_foreground_insert=0 VALUES (1), (2), (3);
SELECT pool, database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name FROM system.background_schedule_pool WHERE database = currentDatabase();
DROP TABLE test_distributed_03745;
DROP TABLE test_local_03745;
