-- The scatter of the final DISTINCT turns every chunk into one chunk per partition, and the preliminary
-- DISTINCT in front of it emits a small chunk whenever it collapses its input. The chunks are squashed
-- before the scatter so that the per-chunk cost of the partitions cannot dominate the step.

SET max_threads = 4;
-- The CI test config sets the global size limits, which disable the parallel final DISTINCT.
SET max_rows_in_distinct = 0, max_bytes_in_distinct = 0;

SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 FROM numbers_mt(1000000))
WHERE explain LIKE '%Squashing%' OR explain LIKE '%Scatter%' OR explain LIKE '%Distinct%';

SELECT 'a merged input is not squashed';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 FROM numbers_mt(1000000) SETTINGS allow_parallel_final_distinct = 0)
WHERE explain LIKE '%Squashing%' OR explain LIKE '%Scatter%' OR explain LIKE '%Distinct%';

DROP TABLE IF EXISTS t_distinct_squash;
CREATE TABLE t_distinct_squash (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_distinct_squash SELECT number % 1000, toString(number % 7) FROM numbers(100000);
INSERT INTO t_distinct_squash SELECT number % 1500 + 500, toString(number % 5) FROM numbers(100000);

-- A block size far below the one the transform squashes to makes it accumulate many chunks per flush.
SELECT 'results';
SELECT count(), sum(a) FROM (SELECT DISTINCT a FROM t_distinct_squash) SETTINGS max_block_size = 1000;
SELECT count(), sum(a) FROM (SELECT DISTINCT a FROM t_distinct_squash) SETTINGS max_block_size = 65536;
SELECT count(), sum(a), sum(cityHash64(b)) FROM (SELECT DISTINCT a, b FROM t_distinct_squash) SETTINGS max_block_size = 1000;
SELECT count(), sum(a), sum(cityHash64(b)) FROM (SELECT DISTINCT a, b FROM t_distinct_squash) SETTINGS max_block_size = 65536;
SELECT count(), sum(x) FROM (SELECT DISTINCT number % 5000 AS x FROM numbers_mt(1000000)) SETTINGS max_block_size = 1000;

DROP TABLE t_distinct_squash;
