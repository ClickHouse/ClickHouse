-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_distinct_limit;
CREATE TABLE t_distinct_limit (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO t_distinct_limit SELECT number FROM numbers(800);

-- Without the optimization the final DISTINCT merges all 8 streams, so the limit applies to the global
-- set of 800 distinct values and the query fails.
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 0, max_rows_in_distinct = 200, max_bytes_in_distinct = 0, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- With the optimization each of the 8 partitions is deduplicated in its own stream, so the limit applies
-- per partition stream (100 distinct values each) and the same query succeeds.
SELECT DISTINCT a FROM t_distinct_limit SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_rows_in_distinct = 200, max_bytes_in_distinct = 0, distinct_overflow_mode = 'throw', max_threads = 8 FORMAT Null;

DROP TABLE t_distinct_limit;
