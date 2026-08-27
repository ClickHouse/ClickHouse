-- Tags: no-random-merge-tree-settings, no-fasttest, no-replicated-database
-- Tag: no-fasttest -- requires S3
-- Tag: no-replicated-database -- ALTER MOVE PARTITION TO should not be replicated (will be fixed separatelly)

SET optimize_trivial_insert_select = 1;

CREATE TABLE test_move_partition_throttling (key UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple() SETTINGS storage_policy='local_remote';
INSERT INTO test_move_partition_throttling SELECT number FROM numbers(1e6);
SELECT disk_name, partition, rows FROM system.parts WHERE database = currentDatabase() AND table = 'test_move_partition_throttling' and active;
ALTER TABLE test_move_partition_throttling MOVE PARTITION tuple() TO VOLUME 'remote' SETTINGS max_remote_write_network_bandwidth=1600000;
SYSTEM FLUSH LOGS query_log;
-- (8e6-1600000)/1600000=4.0
SELECT query_kind, query_duration_ms>4e3 FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query_kind = 'Alter';
SELECT disk_name, partition, rows FROM system.parts WHERE database = currentDatabase() AND table = 'test_move_partition_throttling' and active;
