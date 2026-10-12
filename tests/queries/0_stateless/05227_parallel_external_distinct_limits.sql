SET max_threads = 4, max_block_size = 32;
SET allow_parallel_distinct = 1, allow_distinct_partitions_independently = 0;
SET optimize_distinct_in_order = 0, enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1073741824;
SET max_rows_in_distinct = 1000, max_bytes_in_distinct = 10000000;
SET distinct_overflow_mode = 'throw';

-- Enabling spilling retains parallel final deduplication and one global check for both size limits.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0,
       countIf(explain LIKE '%ExternalDistinctTransform × 4%') = 1,
       countIf(explain LIKE '%DistinctLimitsCheckingTransform%') = 1
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 64 FROM numbers_mt(512));
SELECT count(), uniqExact(k), sum(k)
FROM (SELECT DISTINCT number % 64 AS k FROM numbers_mt(512));

-- The combined result exceeds the row limit although each branch contains only one key.
SELECT materialize(toUInt64(0)) AS k FROM numbers_mt(64)
UNION DISTINCT
SELECT materialize(toUInt64(1)) AS k FROM numbers_mt(64)
SETTINGS max_rows_in_distinct = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Forcing a spill before hashing retains all rows even with a one-byte retained-set limit.
SET max_bytes_before_external_distinct = 1;
SET max_bytes_in_distinct = 1;
SELECT count(), uniqExact(k), sum(k)
FROM (SELECT DISTINCT number % 64 AS k FROM numbers_mt(512));
SELECT count() FROM (SELECT DISTINCT number % 64 AS k FROM numbers_mt(512))
SETTINGS max_rows_in_distinct = 64;

-- Merged spill output observes the global row limit in both overflow modes.
SELECT DISTINCT number % 64 AS k FROM numbers_mt(512)
SETTINGS max_rows_in_distinct = 32 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() BETWEEN 33 AND 64, count() = uniqExact(k)
FROM (SELECT DISTINCT number % 64 AS k FROM numbers_mt(512))
SETTINGS max_rows_in_distinct = 33, distinct_overflow_mode = 'break';

-- Composite and generic keys retain their values through parallel spilling and global accounting.
SELECT count(), sum(k) FROM
(
    SELECT DISTINCT toUInt8(number % 8) AS k, toUInt64(number % 64) AS v FROM numbers_mt(512)
);
SELECT count(), sum(k[1]) FROM
(
    SELECT DISTINCT [number % 64] AS k FROM numbers_mt(512)
);

-- Existing disjoint table partitions can use external deduplication without another scatter.
SET allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1;
CREATE TABLE parallel_external_limits (k UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k % 4;
INSERT INTO parallel_external_limits SELECT number % 64 FROM numbers(512);
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0,
       countIf(explain LIKE '%ExternalDistinctTransform × 4%') = 1,
       countIf(explain LIKE '%DistinctLimitsCheckingTransform%') = 1
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM parallel_external_limits);
SELECT count(), uniqExact(k), sum(k) FROM (SELECT DISTINCT k FROM parallel_external_limits);
SELECT DISTINCT k FROM parallel_external_limits
SETTINGS max_rows_in_distinct = 32 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- An order requirement still keeps the final deduplication in one stream.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0,
       countIf(explain LIKE '%DistinctLimitsCheckingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM (SELECT k FROM parallel_external_limits ORDER BY k));
SELECT groupArray(k) = range(64)
FROM (SELECT DISTINCT k FROM (SELECT k FROM parallel_external_limits ORDER BY k));
DROP TABLE parallel_external_limits;

-- Global cancellation stops idle partitions that suppress an unbounded duplicate-only input.
SET max_bytes_before_external_distinct = 1073741824, max_bytes_in_distinct = 10000000;
SELECT count() = 2 FROM
(
    SELECT materialize(toUInt64(0)) AS k FROM system.numbers_mt
    UNION DISTINCT
    SELECT materialize(toUInt64(1)) AS k FROM system.numbers_mt
)
SETTINGS max_rows_in_distinct = 2, distinct_overflow_mode = 'break', max_execution_time = 10;
