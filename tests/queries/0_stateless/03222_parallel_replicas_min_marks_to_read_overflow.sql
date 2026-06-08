DROP TABLE IF EXISTS test__fuzz_22 SYNC;

CREATE TABLE test__fuzz_22 (k Float32, v String) ENGINE = ReplicatedMergeTree('/clickhouse/03222/{database}/test__fuzz_22', 'r1') ORDER BY k SETTINGS index_granularity = 1;

INSERT INTO test__fuzz_22 SELECT number, toString(number) FROM numbers(10_000);

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT v
FROM test__fuzz_22
ORDER BY v
LIMIT 10, 10
SETTINGS merge_tree_min_rows_for_concurrent_read = 9223372036854775806;

SELECT '---';

SELECT k, v
FROM test__fuzz_22
ORDER BY k
LIMIT 100, 10
SETTINGS optimize_read_in_order=1, merge_tree_min_rows_for_concurrent_read = 9223372036854775806;

DROP TABLE test__fuzz_22 SYNC;
