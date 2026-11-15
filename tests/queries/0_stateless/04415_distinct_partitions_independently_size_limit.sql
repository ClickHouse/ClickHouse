-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_distinct_limit;
CREATE TABLE t_distinct_limit (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO t_distinct_limit SELECT number FROM numbers(800);

-- With partition reuse disabled, final `DISTINCT` checks the global set of 800 distinct values,
-- so the query exceeds the row limit.
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 0, max_rows_in_distinct = 200, max_bytes_in_distinct = 0, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Reusing disjoint partitions keeps the limits global. `DistinctLimitsCheckingTransform` checks the combined
-- set while preserving the streams, so the query still fails when their total exceeds either limit.
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_rows_in_distinct = 200, max_bytes_in_distinct = 0, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_rows_in_distinct = 0, max_bytes_in_distinct = 100, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Without size limits the optimization applies and deduplicates each partition independently; all 800
-- distinct values survive.
SELECT count() FROM (SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_threads = 8, max_rows_in_distinct = 0, max_bytes_in_distinct = 0);

DROP TABLE t_distinct_limit;
