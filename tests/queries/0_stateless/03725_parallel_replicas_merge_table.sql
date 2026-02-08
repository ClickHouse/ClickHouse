-- Tags: no-random-settings, no-random-merge-tree-settings
-- Test parallel replicas support for Merge tables and merge() table function.
-- https://github.com/ClickHouse/ClickHouse/issues/67770
--
-- Single-table Merge: Uses granule-level coordination (proper parallel replicas).
-- Multi-table Merge: Uses per-table coordinators and UNION ALL on initiator.

DROP TABLE IF EXISTS test_mt;
DROP TABLE IF EXISTS test_merge;
DROP TABLE IF EXISTS test_mt_multi_1;
DROP TABLE IF EXISTS test_mt_multi_2;
DROP TABLE IF EXISTS test_merge_multi;

CREATE TABLE test_mt (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_mt SELECT number, toString(number) FROM numbers(10000);

CREATE TABLE test_merge ENGINE = Merge(currentDatabase(), '^test_mt$');

CREATE TABLE test_mt_multi_1 (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE test_mt_multi_2 (k UInt64, v String) ENGINE = MergeTree ORDER BY k;

INSERT INTO test_mt_multi_1 SELECT number, toString(number) FROM numbers(10000);
INSERT INTO test_mt_multi_2 SELECT number + 10000, toString(number + 10000) FROM numbers(10000);

CREATE TABLE test_merge_multi ENGINE = Merge(currentDatabase(), '^test_mt_multi_');

SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_allow_merge_tables = 1;

-- Test 1: Merge table with single underlying table - proper granule coordination
-- With 3 replicas and coordination, we should get 10000 rows (not 30000)
SELECT count(), sum(k) FROM test_merge
SETTINGS log_comment = '03725_merge_table';

-- Test 2: merge() table function - same behavior
SELECT count(), sum(k) FROM merge(currentDatabase(), '^test_mt$')
SETTINGS log_comment = '03725_merge_function';

-- Test 3: Multi-table Merge - per-table coordinators + UNION ALL
SELECT count(), sum(k) FROM test_merge_multi
SETTINGS log_comment = '03725_merge_multi';

-- Test 4: Setting disabled - should NOT use parallel replicas
SELECT count() FROM test_merge
SETTINGS parallel_replicas_allow_merge_tables = 0, log_comment = '03725_disabled';

DROP TABLE test_merge_multi;
DROP TABLE test_mt_multi_2;
DROP TABLE test_mt_multi_1;
DROP TABLE test_merge;
DROP TABLE test_mt;
