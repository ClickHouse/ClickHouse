-- The legacy interpreter must keep the final DISTINCT in one stream when a downstream operation
-- selects rows by its input order.
SET max_threads = 4;
SET enable_analyzer = 0;

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) LIMIT -10);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) LIMIT 0.1);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(100000) OFFSET 10);

SELECT countIf(explain LIKE '%ScatterByPartitionTransform%')
FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT number % 2 AS a, number
    FROM numbers_mt(100000)
    LIMIT 1 BY a
);

SET enable_analyzer = 1;
