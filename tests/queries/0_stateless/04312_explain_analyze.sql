-- Since output for EXPLAIN ANALYZE is non-deterministic, 
-- we only assert structural invariants
-- and deterministic row counts via string matching on the `explain` column.

SELECT count() > 0
FROM (EXPLAIN ANALYZE SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- The query summary block is present.
SELECT
    countIf(explain LIKE '%Query summary:%') = 1,
    countIf(explain LIKE '%Time:%') = 1,
    countIf(explain LIKE '%Read:%') = 1,
    countIf(explain LIKE '%Peak memory:%') = 1
FROM (EXPLAIN ANALYZE SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- Expected step is annotated and per-step fields exist.
SELECT
    countIf(explain LIKE '%Aggregating%') >= 1,
    countIf(explain LIKE '%Actual%parallelism%') >= 1
FROM (EXPLAIN ANALYZE SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- JOIN: both build and probe phases are annotated.
SELECT
    countIf(explain LIKE '%Actual (build)%') >= 1,
    countIf(explain LIKE '%Actual (probe)%') >= 1
FROM (EXPLAIN ANALYZE
    SELECT t1.a
    FROM (SELECT number AS a FROM numbers(1000)) AS t1
    INNER JOIN (SELECT number AS a FROM numbers(1000)) AS t2 ON t1.a = t2.a);