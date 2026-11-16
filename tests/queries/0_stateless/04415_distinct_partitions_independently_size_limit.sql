-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- Some CI configurations set DISTINCT size limits at the server level; pin them to unlimited so that
-- only the per-query SETTINGS below control the behavior.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

DROP TABLE IF EXISTS t_distinct_limit;
CREATE TABLE t_distinct_limit (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO t_distinct_limit SELECT number FROM numbers(800);

-- With the optimization disabled the final DISTINCT merges all 8 streams, so the limit applies to the
-- global set of 800 distinct values and the query fails.
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 0, max_rows_in_distinct = 200, max_bytes_in_distinct = 0, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `max_rows_in_distinct` and `max_bytes_in_distinct` are enforced by the single final transform that
-- sees the whole merged result. When either limit is set, independent per-partition DISTINCT is not
-- applied (even when forced), so the limit keeps its global meaning and the query fails the same way.
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_rows_in_distinct = 200, max_bytes_in_distinct = 0, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_rows_in_distinct = 0, max_bytes_in_distinct = 100, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Without size limits the optimization applies and deduplicates each partition independently; all 800
-- distinct values survive.
SELECT count() FROM (SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_threads = 8);

DROP TABLE t_distinct_limit;
