-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- A materialized CTE and a `GLOBAL IN` subquery are first written into a temporary table while
-- the query runs. The output of EXPLAIN ANALYZE carries timings and is non-deterministic, so the
-- test asserts only that the plan took that path, that concurrency lines were printed, and that
-- with a single thread the reported concurrency never exceeds one.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

SELECT
    countIf(explain LIKE '%MaterializingCTEs%') > 0,
    countIf(explain LIKE '%Concurrency:%') > 0,
    countIf(explain LIKE '%Concurrency:%'
        AND explain NOT LIKE '%step 1.00/1 · branch 1.00/1%'
        AND explain NOT LIKE '%step Unknown · branch 1.00/1%') = 0
FROM (EXPLAIN ANALYZE time = 1
    WITH cte AS MATERIALIZED (SELECT number AS n FROM numbers(200000))
    SELECT count() FROM cte WHERE n IN (SELECT n FROM cte WHERE n % 7 = 1)
    SETTINGS max_threads = 1);

SELECT
    countIf(explain LIKE '%CreatingSets%') > 0,
    countIf(explain LIKE '%Concurrency:%') > 0,
    countIf(explain LIKE '%Concurrency:%'
        AND explain NOT LIKE '%step 1.00/1 · branch 1.00/1%'
        AND explain NOT LIKE '%step Unknown · branch 1.00/1%') = 0
FROM (EXPLAIN ANALYZE time = 1
    SELECT count() FROM numbers(200000) WHERE number GLOBAL IN (SELECT number FROM numbers(1000))
    SETTINGS max_threads = 1);
