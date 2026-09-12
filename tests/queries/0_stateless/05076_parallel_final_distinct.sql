-- The final DISTINCT hash-partitions its input by the DISTINCT columns and deduplicates every
-- stream independently instead of merging the streams into one.

DROP TABLE IF EXISTS t_parallel_distinct;
CREATE TABLE t_parallel_distinct (a UInt64, b String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_parallel_distinct SELECT number % 1000, toString(number % 7) FROM numbers(100000);
INSERT INTO t_parallel_distinct SELECT number % 1500 + 500, toString(number % 5) FROM numbers(100000);

SET max_threads = 4;
-- The CI test config sets the global size limits, which disable the parallel final DISTINCT.
SET max_rows_in_distinct = 0, max_bytes_in_distinct = 0;

SELECT 'results';
SELECT count(), sum(a), sum(length(b)) FROM (SELECT DISTINCT a, b FROM t_parallel_distinct);
SELECT count(), uniqExact(a) FROM (SELECT DISTINCT a FROM t_parallel_distinct);
SELECT count() FROM (SELECT DISTINCT * FROM t_parallel_distinct);
SELECT count() FROM (SELECT DISTINCT a FROM t_parallel_distinct LIMIT 10);
SELECT arraySort(groupArray((a, b))) = (SELECT arraySort(groupArray((a, b))) FROM (SELECT DISTINCT a, b FROM t_parallel_distinct SETTINGS allow_parallel_final_distinct = 0))
FROM (SELECT DISTINCT a, b FROM t_parallel_distinct);

SELECT 'parallel';
SELECT explain FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 FROM numbers_mt(1000000))
WHERE explain LIKE '%Distinct%' OR explain LIKE '%Scatter%' OR explain LIKE '%Resize%';

SELECT 'disabled';
SELECT explain FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 FROM numbers_mt(1000000) SETTINGS allow_parallel_final_distinct = 0)
WHERE explain LIKE '%Distinct%' OR explain LIKE '%Scatter%' OR explain LIKE '%Resize%';

SELECT 'size limits keep the merge';
SELECT explain FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 100 FROM numbers_mt(1000000) SETTINGS max_rows_in_distinct = 1000000)
WHERE explain LIKE '%Distinct%' OR explain LIKE '%Scatter%' OR explain LIKE '%Resize%';

SELECT 'disjoint partition streams';
DROP TABLE IF EXISTS t_parallel_distinct_part;
CREATE TABLE t_parallel_distinct_part (a UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO t_parallel_distinct_part SELECT number FROM numbers(1000);
SELECT explain FROM (EXPLAIN PIPELINE SELECT DISTINCT a FROM t_parallel_distinct_part SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_threads = 8, enable_parallel_replicas = 0)
WHERE explain LIKE '%Distinct%' OR explain LIKE '%Scatter%' OR explain LIKE '%Resize%';

SELECT 'sorted input is not scattered';
DROP TABLE IF EXISTS t_parallel_distinct_sorted;
CREATE TABLE t_parallel_distinct_sorted (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_parallel_distinct_sorted SELECT number % 1000 FROM numbers(100000);
-- The read pipeline above the DISTINCT (virtual rows, in-order merges, the number of streams) depends on the
-- settings, so only the transforms matter.
SELECT replaceRegexpOne(trimLeft(explain), ' × \\d+$', '') FROM (EXPLAIN PIPELINE SELECT DISTINCT a FROM t_parallel_distinct_sorted ORDER BY a SETTINGS optimize_distinct_in_order = 1)
WHERE explain LIKE '%Distinct%' OR explain LIKE '%Scatter%';

DROP TABLE t_parallel_distinct;
DROP TABLE t_parallel_distinct_part;
DROP TABLE t_parallel_distinct_sorted;
