-- The final DISTINCT deduplicates its input streams in parallel by repartitioning them by the hash of
-- the DISTINCT columns, instead of merging everything into a single stream and deduplicating there.

SET max_threads = 4;

SELECT '-- the pipeline scatters by hash and deduplicates every stream';
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0, countIf(explain LIKE '%DistinctTransform × 4%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 FROM numbers_mt(10000000));

SELECT '-- and merges into one stream when the optimization is disabled';
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0, countIf(explain LIKE '%Resize 4 → 1%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 FROM numbers_mt(10000000) SETTINGS allow_parallel_distinct = 0);

SELECT '-- constant DISTINCT columns are not scattered';
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT 1 FROM numbers_mt(10000000));

SELECT '-- a LIMIT keeps the single stream, so it keeps returning the first values of the input';
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000000) LIMIT 100);

SELECT '-- the limits on the size of the DISTINCT set stay global';
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 FROM numbers_mt(10000000) SETTINGS max_rows_in_distinct = 1000000);
SELECT count() FROM (SELECT DISTINCT number % 1000 FROM numbers_mt(10000000) SETTINGS max_rows_in_distinct = 100); -- { serverError SET_SIZE_LIMIT_EXCEEDED }
-- 1000 distinct values scattered over 4 streams: a per-stream limit of 500 would let this pass, a global one does not.
SELECT count() FROM (SELECT DISTINCT number % 1000 FROM numbers_mt(10000000) SETTINGS max_rows_in_distinct = 500); -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT DISTINCT number % 1000 FROM numbers_mt(10000000) SETTINGS max_bytes_in_distinct = 1); -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE IF EXISTS t_parallel_distinct;

CREATE TABLE t_parallel_distinct (a UInt64, b UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY a;

-- Several parts with overlapping ranges of `a`, so the streams are not disjoint by themselves.
INSERT INTO t_parallel_distinct SELECT number % 1000, number % 7, toString(number % 13) FROM numbers(10000);
INSERT INTO t_parallel_distinct SELECT number % 1000, number % 7, toString(number % 13) FROM numbers(10000);
INSERT INTO t_parallel_distinct SELECT number % 1500, number % 7, toString(number % 13) FROM numbers(10000);
INSERT INTO t_parallel_distinct SELECT number % 1500, number % 7, toString(number % 13) FROM numbers(10000);

SELECT '-- the same result with and without the optimization';
SELECT count(), sum(a), min(a), max(a) FROM (SELECT DISTINCT a FROM t_parallel_distinct);
SELECT count(), sum(a), min(a), max(a) FROM (SELECT DISTINCT a FROM t_parallel_distinct SETTINGS allow_parallel_distinct = 0);

SELECT '-- several columns, including LowCardinality';
SELECT count(), sum(a + b), sum(length(s)) FROM (SELECT DISTINCT a, b, s FROM t_parallel_distinct);
SELECT count(), sum(a + b), sum(length(s)) FROM (SELECT DISTINCT a, b, s FROM t_parallel_distinct SETTINGS allow_parallel_distinct = 0);

SELECT '-- DISTINCT *';
SELECT count() FROM (SELECT DISTINCT * FROM t_parallel_distinct);
SELECT count() FROM (SELECT DISTINCT * FROM t_parallel_distinct SETTINGS allow_parallel_distinct = 0);

SELECT '-- DISTINCT of a Nullable column';
SELECT count(), countIf(a IS NULL) FROM (SELECT DISTINCT if(a % 100 = 0, NULL, a) AS a FROM t_parallel_distinct);
SELECT count(), countIf(a IS NULL) FROM (SELECT DISTINCT if(a % 100 = 0, NULL, a) AS a FROM t_parallel_distinct SETTINGS allow_parallel_distinct = 0);

SELECT '-- a globally sorted input keeps its order';
SELECT groupArray(a) = arraySort(groupArray(a)) FROM (SELECT DISTINCT a FROM (SELECT a FROM t_parallel_distinct ORDER BY a));

SELECT '-- ORDER BY after DISTINCT still sorts';
SELECT groupArray(a) = arraySort(groupArray(a)) FROM (SELECT DISTINCT a FROM t_parallel_distinct ORDER BY a);

DROP TABLE t_parallel_distinct;
