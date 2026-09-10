SET query_plan_convert_distinct_to_aggregation = 1;
SET distinct_overflow_mode = 'throw';
SET max_threads = 4;
SET max_block_size = 1000;
SET query_plan_remove_redundant_distinct = 0;

-- Unbounded prepared sources retain `DISTINCT`; finite values use aggregation.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT x FROM generateRandom('x UInt64'));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT x FROM values('x UInt8', 1, 2, 1));

-- Every positional consumer above `DISTINCT` retains streaming deduplication.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) OFFSET -3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) OFFSET 0.1);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) LIMIT -1 BY number % 3);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT number FROM numbers_mt(10000)) LIMIT AFTER number >= 3);

-- A positional limit below `DISTINCT` does not constrain the replacement of its consumer.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM (SELECT number FROM numbers_mt(10000) LIMIT 1000));

-- A limit in one union branch does not constrain an independent branch's `DISTINCT`.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE (SELECT number FROM numbers(10) LIMIT 3) UNION ALL (SELECT DISTINCT number FROM numbers_mt(10000)));

-- Extremes computed after `DISTINCT` remain compatible with the replacement.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0, countIf(explain LIKE '%ExtremesTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(10000) SETTINGS extremes = 1);
SELECT k FROM (SELECT DISTINCT number % 3 AS k FROM numbers_mt(10000)) ORDER BY k SETTINGS extremes = 1;
