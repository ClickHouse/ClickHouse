-- Tags: no-fasttest
DROP TABLE IF EXISTS 03231_max_execution_time_t SYNC;
CREATE TABLE 03231_max_execution_time_t
(
    key UInt64,
    value String,
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/03231_max_execution_time', 'r1')
ORDER BY (key, value);

SET max_rows_to_read = 20_000_000;
SYSTEM STOP MERGES 03231_max_execution_time_t;
INSERT INTO 03231_max_execution_time_t SELECT number, toString(number) FROM numbers_mt(20_000_000) SETTINGS max_threads=0, max_insert_threads=0;

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SET use_query_cache = false;

SELECT key, SUM(length(value)) FROM 03231_max_execution_time_t GROUP BY key FORMAT Null SETTINGS max_execution_time=1; -- { serverError TIMEOUT_EXCEEDED }
SELECT key, SUM(length(value)) FROM 03231_max_execution_time_t GROUP BY key FORMAT Null SETTINGS max_execution_time_leaf=1; -- { serverError TIMEOUT_EXCEEDED }
-- Can return partial result
SELECT key, SUM(length(value)) FROM 03231_max_execution_time_t GROUP BY key FORMAT Null SETTINGS max_execution_time=1, timeout_overflow_mode='break';
SELECT key, SUM(length(value)) FROM 03231_max_execution_time_t GROUP BY key FORMAT Null SETTINGS max_execution_time_leaf=1, timeout_overflow_mode_leaf='break';

DROP TABLE 03231_max_execution_time_t SYNC;
