-- LIMIT and OFFSET that are resolved after all rows are read preserve the input order after the
-- final DISTINCT, so they must keep the single-stream pipeline.
SET max_threads = 4;

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) LIMIT -10);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) LIMIT 0.1);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) OFFSET 10);
