-- Since output for EXPLAIN ANALYZE is non-deterministic,
-- we only assert structural invariants
-- and deterministic row counts via string matching on the `explain` column.

SET enable_analyzer = 1;

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
    countIf(explain LIKE '%parallelism%') >= 1
FROM (EXPLAIN ANALYZE SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- JOIN: both build and probe phases are annotated.
SELECT
    countIf(explain LIKE '%(build): time%') >= 1,
    countIf(explain LIKE '%(probe): time%') >= 1
FROM (EXPLAIN ANALYZE
    SELECT t1.a
    FROM (SELECT number AS a FROM numbers(1000)) AS t1
    INNER JOIN (SELECT number AS a FROM numbers(1000)) AS t2 ON t1.a = t2.a);

-- Aggregating is two-stage: the AggregatingTransform's own partial-aggregation stage and the
-- final-aggregation stage of the merge processors it generates.
SELECT
    countIf(explain LIKE '%(partial aggregation): time%') >= 1,
    countIf(explain LIKE '%(final aggregation): time%') >= 1
FROM (EXPLAIN ANALYZE
    SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- With `processors = 1` every group line is followed by exactly one distribution line
-- that reports the processor count and the min/median/max/sum of the elapsed time.
SELECT
    countIf(explain LIKE '%Time per processor%') >= 1,
    countIf(explain LIKE '%Time per processor (%):%min %· median %· max %· sum %') = countIf(explain LIKE '%Time per processor%'),
    countIf(explain LIKE '%Time per processor%') = countIf(explain LIKE '%parallelism%')
FROM (EXPLAIN ANALYZE processors = 1
    SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);

-- A set-creating subquery (IN) must not crash EXPLAIN ANALYZE: the CreatingSet step
-- is described after the pipeline has consumed it.
SELECT
    countIf(explain LIKE '%CreatingSet%') >= 1
FROM (EXPLAIN ANALYZE
    SELECT number FROM numbers(1000) WHERE number IN (SELECT number FROM numbers(10)));
