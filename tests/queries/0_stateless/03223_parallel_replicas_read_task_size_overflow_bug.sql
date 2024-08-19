DROP TABLE IF EXISTS test__fuzz_22 SYNC;

CREATE TABLE test__fuzz_22 (k Float32, v String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;

SYSTEM STOP MERGES test__fuzz_22;

INSERT INTO test__fuzz_22 SELECT number, toString(number) FROM numbers(1);
INSERT INTO test__fuzz_22 SELECT number, toString(number) FROM numbers(1);
INSERT INTO test__fuzz_22 SELECT number, toString(number) FROM numbers(1);
INSERT INTO test__fuzz_22 SELECT number, toString(number) FROM numbers(1);

SET allow_experimental_parallel_reading_from_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost',
    merge_tree_min_rows_for_concurrent_read = 9223372036854775806, merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 9223372036854775806;

  SELECT v
    FROM test__fuzz_22
ORDER BY v
   LIMIT 10, 10
SETTINGS max_threads = 4
  FORMAT Null; -- { serverError BAD_ARGUMENTS }

DROP TABLE test__fuzz_22 SYNC;
