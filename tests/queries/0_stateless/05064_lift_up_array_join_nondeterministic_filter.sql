-- `ARRAY JOIN` multiplies the rows, so a filter holding a non-deterministic function must not be moved
-- below it: there it is evaluated once per source row instead of once per expanded row.

-- 100 source rows expand to 400, so the predicate keeps 100 of them when it runs above the ARRAY JOIN
-- and all 400 when it runs below it.
SELECT
    (SELECT count() FROM (SELECT number AS g, arr FROM (SELECT number, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE rowNumberInAllBlocks() < 100)) AS lift_allowed,
    (SELECT count() FROM (SELECT number AS g, arr FROM (SELECT number, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE rowNumberInAllBlocks() < 100) SETTINGS query_plan_lift_up_array_join = 0) AS lift_disabled;

-- The filter stays above the ARRAY JOIN ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'ArrayJoin'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT g, arr FROM (SELECT number AS g, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE rand(g) % 2 = 0);

-- ... and a deterministic one is still moved below it.
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'ArrayJoin'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT g, arr FROM (SELECT number AS g, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE g % 2 = 0);
