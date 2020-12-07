--- Put it into tests/queries/0_stateless/01581_deduplicate_by_columns.sql 

--- replicated case

-- Just in case if previous tests run left some stuff behind.
DROP TABLE IF EXISTS replicated_deduplicate_by_columns_r1;
DROP TABLE IF EXISTS replicated_deduplicate_by_columns_r2;

SET replication_alter_partitions_sync = 2;

-- IRL insert_replica_id were filled from hostname
CREATE TABLE IF NOT EXISTS replicated_deduplicate_by_columns_r1 (
	id Int32, val UInt32, insert_time_ns DateTime64(9) MATERIALIZED now64(9), insert_replica_id UInt8 MATERIALIZED randConstant()
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_01581/replicated_deduplicate', 'r1') ORDER BY id;

CREATE TABLE IF NOT EXISTS replicated_deduplicate_by_columns_r2 (
    id Int32, val UInt32, insert_time_ns DateTime64(9) MATERIALIZED now64(9), insert_replica_id UInt8 MATERIALIZED randConstant()
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_01581/replicated_deduplicate', 'r2') ORDER BY id;

-- insert some data, 2 records: (3, 1003), (4, 1004) are duplicated and have difference in insert_time_ns / insert_replica_id
-- (1, 1001), (5, 2005) has full duplicates
INSERT INTO replicated_deduplicate_by_columns_r1 VALUES (1, 1001), (1, 1001), (2, 1002), (3, 1003), (4, 1004), (1, 2001), (9, 1002);
INSERT INTO replicated_deduplicate_by_columns_r2 VALUES (2, 2002), (3, 1003), (4, 1004), (5, 2005), (5, 2005);

-- wait for syncing replicas
SYSTEM SYNC REPLICA replicated_deduplicate_by_columns_r2;
SYSTEM SYNC REPLICA replicated_deduplicate_by_columns_r1;

SELECT 'check that we have a data';
SELECT 'r1', id, val, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) FROM replicated_deduplicate_by_columns_r1 GROUP BY id, val ORDER BY id, val;
SELECT 'r2', id, val, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) FROM replicated_deduplicate_by_columns_r2 GROUP BY id, val ORDER BY id, val;

-- NOTE: here and below we need FINAL to force deduplication in such a small set of data in only 1 part.
-- that should remove full duplicates
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE;

SELECT 'after old OPTIMIZE DEDUPLICATE';
SELECT 'r1', id, val, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) FROM replicated_deduplicate_by_columns_r1 GROUP BY id, val ORDER BY id, val;
SELECT 'r2', id, val, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) FROM replicated_deduplicate_by_columns_r2 GROUP BY id, val ORDER BY id, val;

OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY id, val;
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY COLUMNS('[id, val]');
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY * EXCEPT(insert_time_ns, insert_replica_id);
OPTIMIZE TABLE replicated_deduplicate_by_columns_r1 FINAL DEDUPLICATE BY COLUMNS('[i]') EXCEPT(insert_time_ns, insert_replica_id);

SELECT 'check data again after multiple deduplications with new syntax';
SELECT 'r1', id, val, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) FROM replicated_deduplicate_by_columns_r1 GROUP BY id, val ORDER BY id, val;
SELECT 'r2', id, val, count(), uniqExact(insert_time_ns), uniqExact(insert_replica_id) FROM replicated_deduplicate_by_columns_r2 GROUP BY id, val ORDER BY id, val;

-- cleanup the mess
DROP TABLE replicated_deduplicate_by_columns_r1;
DROP TABLE replicated_deduplicate_by_columns_r2;

SYSTEM DROP REPLICA '/clickhouse/tables/test_01581/replicated_deduplicate/replicas/r1';
SYSTEM DROP REPLICA '/clickhouse/tables/test_01581/replicated_deduplicate/replicas/r2';
