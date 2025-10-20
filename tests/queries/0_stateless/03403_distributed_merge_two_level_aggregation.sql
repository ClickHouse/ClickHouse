-- Tags: no-fasttest, long

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1(number UInt64) ENGINE = MergeTree ORDER BY number;
SYSTEM STOP MERGES test_table_1;

DROP TABLE IF EXISTS dist_test_table_1;
CREATE TABLE dist_test_table_1(number UInt64) ENGINE = Distributed('test_cluster_thirty_shards_localhost', currentDatabase(), test_table_1, rand());
INSERT INTO dist_test_table_1 SELECT number from numbers_mt(10000) SETTINGS distributed_foreground_insert = 1;

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2(number UInt64) ENGINE = MergeTree ORDER BY number;
SYSTEM STOP MERGES test_table_2;

DROP TABLE IF EXISTS dist_test_table_2;
CREATE TABLE dist_test_table_2(number UInt64) ENGINE = Distributed('test_cluster_thirty_shards_localhost', currentDatabase(), test_table_2, rand());
INSERT INTO dist_test_table_2 SELECT number from numbers_mt(10000) SETTINGS distributed_foreground_insert = 1;

DROP TABLE IF EXISTS merge_test_table;
CREATE TABLE merge_test_table ENGINE = Merge(currentDatabase(), '^dist_test_table_(1|2)$');

EXPLAIN PIPELINE
SELECT
    cityHash64(number),
    sum(1)
FROM remote('127.0.0.{1,1}', currentDatabase(), merge_test_table)
GROUP BY 1
SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, optimize_aggregation_in_order = 0, prefer_localhost_replica = 1, async_socket_for_remote = 1, enable_analyzer = 0, enable_producing_buckets_out_of_order_in_aggregation = 0, enable_memory_bound_merging_of_aggregation_results = 0;

SELECT
    cityHash64(number),
    sum(1)
FROM remote('127.0.0.{1,1}', currentDatabase(), merge_test_table)
GROUP BY 1
FORMAT Null
SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, optimize_aggregation_in_order = 0, prefer_localhost_replica = 1, async_socket_for_remote = 1, enable_analyzer = 0, enable_producing_buckets_out_of_order_in_aggregation = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_memory_usage='500Mi', group_by_two_level_threshold=1e6, group_by_two_level_threshold_bytes='500Mi';

DROP TABLE merge_test_table;
DROP TABLE dist_test_table_1;
DROP TABLE dist_test_table_2;
DROP TABLE test_table_1;
DROP TABLE test_table_2;
