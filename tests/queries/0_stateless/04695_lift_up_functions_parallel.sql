-- The expression lifted above `SortingStep` is evaluated by several threads, which must not change the order of the rows.

SET query_plan_execute_functions_after_sorting = 1;
SET max_threads = 8, max_block_size = 1000;

SELECT count() > 0
FROM (EXPLAIN PIPELINE
    SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
    FROM numbers_mt(50000) ORDER BY k % 977 DESC, k)
WHERE explain LIKE '%AddSequenceNumber%';

SELECT g = arraySort(g)
FROM (SELECT groupArray((-toInt32(k % 977), k, a)) AS g FROM (
    SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
    FROM numbers_mt(50000) ORDER BY k % 977 DESC, k));

-- The parallel section must also stop cleanly when a `LIMIT` closes the pipeline early.
SELECT g = arraySort(g)
FROM (SELECT groupArray((-toInt32(k % 977), k, a)) AS g FROM (
    SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
    FROM numbers_mt(50000) ORDER BY k % 977 DESC, k LIMIT 1000));

-- A stateful function keeps the lifted part on a single stream: the streams reach it in an arbitrary
-- order, which the chunk sequence numbers cannot undo.
SELECT count() > 0
FROM (EXPLAIN PIPELINE
    SELECT number AS k, rowNumberInAllBlocks() AS r
    FROM numbers_mt(50000) ORDER BY k % 977 DESC, k)
WHERE explain LIKE '%AddSequenceNumber%';

-- `mergeExpressions` can fold an outer expression into the lifted one, so the same restriction has to
-- hold for the merged expression.
SELECT count() > 0
FROM (EXPLAIN PIPELINE
    SELECT rowNumberInAllBlocks() AS r, a
    FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(50000) ORDER BY k % 977 DESC, k))
WHERE explain LIKE '%AddSequenceNumber%';

SELECT count() > 0
FROM (EXPLAIN PIPELINE
    SELECT rand() AS r, a
    FROM (
        SELECT number AS k, arrayMap(i -> sipHash64(i, k), range(4)) AS a
        FROM numbers_mt(50000) ORDER BY k % 977 DESC, k))
WHERE explain LIKE '%AddSequenceNumber%';

-- `arrayJoin` in the lifted part changes the number of rows in a chunk.
SELECT g = arraySort(g)
FROM (SELECT groupArray((k, n, e)) AS g FROM (
    SELECT k, n, arrayJoin(arr) AS e
    FROM (SELECT number AS n, number % 100 AS k, [number, number + 1, number + 2] AS arr FROM numbers_mt(50000))
    ORDER BY k, n));
