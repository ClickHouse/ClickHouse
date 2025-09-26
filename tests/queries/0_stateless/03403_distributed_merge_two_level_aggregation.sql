-- Tags: no-fasttest

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1(number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO test_table_1 SELECT number from numbers(1000000);
CREATE TABLE dist_test_table_1(number UInt64) ENGINE = Distributed('test_cluster_thirty_shards_localhost', currentDatabase(), test_table_1);

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2(number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO test_table_2 SELECT number from numbers(1000000);
CREATE TABLE dist_test_table_2(number UInt64) ENGINE = Distributed('test_cluster_thirty_shards_localhost', currentDatabase(), test_table_2);

EXPLAIN PIPELINE
SELECT
    cityHash64(number),
    sum(1)
FROM merge('^dist_test_table_(1|2)$')
GROUP BY 1
SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, optimize_aggregation_in_order = 0, async_socket_for_remote = 1, enable_analyzer = 0, enable_producing_buckets_out_of_order_in_aggregation = 0;

SELECT
    cityHash64(number),
    sum(1)
FROM merge('^dist_test_table_(1|2)$')
GROUP BY 1
FORMAT Null
SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, optimize_aggregation_in_order = 0, async_socket_for_remote = 1, enable_analyzer = 0, enable_producing_buckets_out_of_order_in_aggregation = 0;

DROP TABLE dist_test_table_1;
DROP TABLE dist_test_table_2;
DROP TABLE test_table_1;
DROP TABLE test_table_2;
