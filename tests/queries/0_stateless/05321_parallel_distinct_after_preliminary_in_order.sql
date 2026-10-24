-- A preliminary `DISTINCT` in input order emits small chunks with the first row of every run of equal keys.
-- Scattering them for the parallel final `DISTINCT` costs more than it saves, so the final step merges its
-- inputs into one stream. A preliminary `DISTINCT` by hashing still allows the scatter.

SET max_threads = 4, enable_parallel_replicas = 0, allow_parallel_distinct = 1;
SET allow_distinct_partitions_independently = 0, read_in_order_use_virtual_row = 0;
SET max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 0;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_distinct_in_order;
CREATE TABLE t_distinct_in_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
-- Several parts with overlapping key ranges are read in several streams.
SYSTEM STOP MERGES t_distinct_in_order;
INSERT INTO t_distinct_in_order SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_distinct_in_order SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_distinct_in_order SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_distinct_in_order SELECT number % 1000, number FROM numbers(100000);

SELECT countIf(explain LIKE '%DistinctSortedStreamTransform × 4%') > 0, countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT k FROM t_distinct_in_order SETTINGS optimize_distinct_in_order = 1);
SELECT countIf(explain LIKE '%DistinctSortedStreamTransform%'), countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 FROM numbers_mt(10000000));

SELECT count(), sum(k) FROM (SELECT DISTINCT k FROM t_distinct_in_order) SETTINGS optimize_distinct_in_order = 1;
SELECT count(), sum(k) FROM (SELECT DISTINCT k FROM t_distinct_in_order) SETTINGS optimize_distinct_in_order = 0;

DROP TABLE t_distinct_in_order;
