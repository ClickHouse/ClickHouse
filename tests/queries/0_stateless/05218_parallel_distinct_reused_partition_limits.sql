SET max_threads = 4;
SET max_block_size = 16;
SET enable_parallel_replicas = 0;
SET allow_parallel_distinct = 1;
SET allow_distinct_partitions_independently = 1;
SET force_distinct_partitions_independently = 1;
SET allow_preliminary_distinct_abandoning = 0;
SET optimize_distinct_in_order = 0;
SET query_plan_remove_redundant_distinct = 0;
SET max_rows_in_distinct = 1000;
SET max_bytes_in_distinct = 10000000;
SET distinct_overflow_mode = 'throw';
SET max_execution_time = 10;

CREATE TABLE distinct_reused_limits (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 4;
INSERT INTO distinct_reused_limits SELECT number % 200 FROM numbers(2000);

-- Final `DISTINCT` reuses table partitions and checks their combined size without another scatter.
SELECT countIf(explain LIKE '%Skip stream merging: 1%') = 1,
       countIf(explain LIKE '%Read each partition through separate port%') = 1
FROM (EXPLAIN actions = 1 SELECT DISTINCT k FROM distinct_reused_limits);
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0,
       countIf(explain LIKE '%DistinctLimitTransform%') = 1
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM distinct_reused_limits);
SELECT count(), uniqExact(k), sum(k) FROM (SELECT DISTINCT k FROM distinct_reused_limits);
SELECT count() FROM (SELECT DISTINCT k FROM distinct_reused_limits) SETTINGS allow_parallel_distinct = 0;

-- Each partition fits individually, but their combined rows or retained bytes exceed the limit.
SELECT DISTINCT k FROM distinct_reused_limits SETTINGS max_rows_in_distinct = 100 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT k FROM distinct_reused_limits SETTINGS max_bytes_in_distinct = 6144 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT k FROM distinct_reused_limits SETTINGS allow_parallel_distinct = 0, max_rows_in_distinct = 100 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT k FROM distinct_reused_limits SETTINGS allow_parallel_distinct = 0, max_bytes_in_distinct = 6144 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Global `BREAK` emits the chunk reaching the limit and closes all partitions.
SELECT count() BETWEEN 100 AND 115 FROM (SELECT DISTINCT k FROM distinct_reused_limits)
SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break';
SELECT count() > 0 AND count() < 200 FROM (SELECT DISTINCT k FROM distinct_reused_limits)
SETTINGS max_bytes_in_distinct = 6144, distinct_overflow_mode = 'break';

-- Sorted preliminary deduplication still feeds one global limit check after final hash deduplication.
SELECT countIf(explain LIKE '%DistinctSortedStreamTransform%') > 0,
       countIf(explain LIKE '%DistinctLimitTransform%') = 1
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM distinct_reused_limits)
SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM (SELECT DISTINCT k FROM distinct_reused_limits) SETTINGS optimize_distinct_in_order = 1;
SELECT DISTINCT k FROM distinct_reused_limits SETTINGS optimize_distinct_in_order = 1, max_rows_in_distinct = 100 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT k FROM distinct_reused_limits SETTINGS optimize_distinct_in_order = 1, max_bytes_in_distinct = 6144 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- A row limit equal to the combined cardinality succeeds in `THROW` mode.
SELECT count() FROM (SELECT DISTINCT k FROM distinct_reused_limits)
SETTINGS max_rows_in_distinct = 200;

-- A key that does not determine the table partition still requires cross-stream deduplication.
SELECT countIf(explain LIKE '%Skip stream merging: 1%') = 0
FROM (EXPLAIN actions = 1 SELECT DISTINCT k % 2 FROM distinct_reused_limits);
SELECT count() FROM (SELECT DISTINCT k % 2 FROM distinct_reused_limits);

-- A single surviving partition uses a local final limit check, and an empty read returns no keys.
SELECT countIf(explain LIKE '%DistinctLimitTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM distinct_reused_limits WHERE k % 4 = 0);
SELECT count() FROM (SELECT DISTINCT k FROM distinct_reused_limits WHERE k % 4 = 0);
SELECT count() FROM (SELECT DISTINCT k FROM distinct_reused_limits WHERE k > 1000);
SELECT count(), uniqExact(k) FROM (SELECT DISTINCT k FROM distinct_reused_limits LIMIT 7);

-- Globally ordered input retains its order with either final deduplication algorithm.
SELECT groupArray(k) = range(200)
FROM (SELECT DISTINCT k FROM (SELECT k FROM distinct_reused_limits ORDER BY k));
SELECT groupArray(k) = range(200)
FROM (SELECT DISTINCT k FROM (SELECT k FROM distinct_reused_limits ORDER BY k))
SETTINGS optimize_distinct_in_order = 1;

-- A second `DISTINCT` reuses the first scatter while each step has its own global size limit.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 1,
       countIf(explain LIKE '%DistinctLimitTransform%') = 2
FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT k, x
    FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(10000))
    ARRAY JOIN [0, 0, 1] AS x
);
SELECT count(), sum(c) FROM
(
    SELECT k, x, count() OVER (PARTITION BY k) AS c FROM
    (
        SELECT DISTINCT k, x
        FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 0, 1] AS x
    )
)
SETTINGS allow_window_partitions_independently = 1,
    query_plan_enable_multithreading_after_window_functions = 0;

-- A reused hash partition can stop an unbounded input whose lower deduplication suppresses new chunks.
SELECT count() = 2 FROM
(
    SELECT DISTINCT k
    FROM
    (
        SELECT DISTINCT number % 2 AS k FROM system.numbers_mt
        SETTINGS max_rows_in_distinct = 0, max_bytes_in_distinct = 0
    )
    ARRAY JOIN [0, 1] AS x
)
SETTINGS max_rows_in_distinct = 2, distinct_overflow_mode = 'break';

DROP TABLE distinct_reused_limits;
