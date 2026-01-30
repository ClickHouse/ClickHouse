-- Tags: no-fasttest, no-asan

DROP TABLE IF EXISTS test_max_execution_time_leaf SYNC;
CREATE TABLE test_max_execution_time_leaf
(
    key UInt64,
    value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_max_execution_time_leaf', 'r1')
ORDER BY (key, value);

SET max_rows_to_read = 0;
SYSTEM STOP MERGES test_max_execution_time_leaf;
INSERT INTO test_max_execution_time_leaf SELECT number, toString(number) FROM numbers_mt(20000000) SETTINGS max_threads=0, max_insert_threads=0;

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SET use_query_cache = false;

SELECT key, SUM(length(value)) FROM test_max_execution_time_leaf GROUP BY key FORMAT Null SETTINGS max_execution_time=1; -- { serverError TIMEOUT_EXCEEDED }
SELECT key, SUM(length(value)) FROM test_max_execution_time_leaf GROUP BY key FORMAT Null SETTINGS max_execution_time_leaf=1; -- { serverError TIMEOUT_EXCEEDED }
-- Can return partial result
SELECT key, SUM(length(value)) FROM test_max_execution_time_leaf GROUP BY key FORMAT Null SETTINGS max_execution_time=1, timeout_overflow_mode='break';
SELECT key, SUM(length(value)) FROM test_max_execution_time_leaf GROUP BY key FORMAT Null SETTINGS max_execution_time_leaf=1, timeout_overflow_mode_leaf='break';

DROP TABLE test_max_execution_time_leaf SYNC;
