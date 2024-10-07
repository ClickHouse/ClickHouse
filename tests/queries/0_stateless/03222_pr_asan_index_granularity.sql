DROP TABLE IF EXISTS test;

CREATE TABLE test (k UInt64, v String)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity=1;

INSERT INTO test SELECT number, toString(number) FROM numbers(10_000);

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT 0, materialize(18), k FROM test PREWHERE toNullable(toNullable(11)) WHERE toNullable(11) ORDER BY k DESC NULLS LAST LIMIT 100, 100 SETTINGS optimize_read_in_order = 1, merge_tree_min_rows_for_concurrent_read = 9223372036854775806, max_threads = 1;

-- DROP TABLE test;
