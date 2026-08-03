-- The expression lifted above `SortingStep` is evaluated by several threads, which must not change the order of the rows.

SET query_plan_execute_functions_after_sorting = 1;

SELECT count() > 0
FROM (EXPLAIN PIPELINE
    SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
    FROM numbers_mt(1000000) ORDER BY k % 977 DESC, k
    SETTINGS max_threads = 8)
WHERE explain LIKE '%OrderedScatter%';

SELECT
    (SELECT groupArray(a) FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(300000) ORDER BY k % 977 DESC, k
        SETTINGS max_threads = 8))
  = (SELECT groupArray(a) FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(300000) ORDER BY k % 977 DESC, k
        SETTINGS max_threads = 1, query_plan_execute_functions_after_sorting = 0));

-- The parallel section must also stop cleanly when a `LIMIT` closes the pipeline early.
SELECT
    (SELECT groupArray(a) FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(300000) ORDER BY k % 977 DESC, k LIMIT 1000
        SETTINGS max_threads = 8))
  = (SELECT groupArray(a) FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(300000) ORDER BY k % 977 DESC, k LIMIT 1000
        SETTINGS max_threads = 1, query_plan_execute_functions_after_sorting = 0));

-- A stateful function keeps the lifted part on a single stream: the streams reach it in an arbitrary
-- order, which `OrderedGather` cannot undo.
SELECT count()
FROM (EXPLAIN PIPELINE
    SELECT number AS k, rowNumberInAllBlocks() AS r
    FROM numbers_mt(1000000) ORDER BY k % 977 DESC, k
    SETTINGS max_threads = 8)
WHERE explain LIKE '%OrderedScatter%';

-- `mergeExpressions` can fold an outer expression into the lifted one, so the same restriction has to
-- be re-checked on the merged expression.
SELECT count()
FROM (EXPLAIN PIPELINE
    SELECT rowNumberInAllBlocks() AS r, a
    FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(1000000) ORDER BY k % 977 DESC, k)
    SETTINGS max_threads = 8)
WHERE explain LIKE '%OrderedScatter%';

SELECT count()
FROM (EXPLAIN PIPELINE
    SELECT rand() AS r, a
    FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(1000000) ORDER BY k % 977 DESC, k)
    SETTINGS max_threads = 8)
WHERE explain LIKE '%OrderedScatter%';

-- `arrayJoin` in the lifted part changes the number of rows in a chunk.
SELECT
    (SELECT groupArray(e) FROM (
        SELECT k, arrayJoin(arr) AS e
        FROM (SELECT number AS n, number % 100 AS k, [number, number + 1, number + 2] AS arr FROM numbers_mt(300000))
        ORDER BY k, n
        SETTINGS max_threads = 8))
  = (SELECT groupArray(e) FROM (
        SELECT k, arrayJoin(arr) AS e
        FROM (SELECT number AS n, number % 100 AS k, [number, number + 1, number + 2] AS arr FROM numbers_mt(300000))
        ORDER BY k, n
        SETTINGS max_threads = 1));
