-- A lambda is as deterministic and as stateful as its body. A higher-order function with a non-deterministic
-- lambda must not be moved across an `ARRAY JOIN`, neither by `liftUpArrayJoin` nor by the filter pushdown:
-- below the `ARRAY JOIN`, the lambda is evaluated once per source row instead of once per expanded row.

-- 100 source rows expand to 400, so the predicate keeps 100 of them when it runs above the ARRAY JOIN
-- and all 400 when it runs below it.
SELECT
    (SELECT count() FROM (SELECT number AS g, arr FROM (SELECT number, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE arrayExists(x -> rowNumberInAllBlocks() < 100, range(1)))) AS lift_up_route,
    (SELECT count() FROM (SELECT number AS g, arr FROM (SELECT number, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr) WHERE arrayExists(x -> rowNumberInAllBlocks() < 100, range(1))) AS push_down_route,
    (SELECT count() FROM (SELECT number AS g, arr FROM (SELECT number, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE arrayExists(x -> rowNumberInAllBlocks() < 100, range(1))) SETTINGS query_plan_lift_up_array_join = 0, query_plan_filter_push_down = 0) AS no_optimizations;

-- The filter with a non-deterministic lambda stays above the ARRAY JOIN ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'ArrayJoin'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT g, arr FROM (SELECT number AS g, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE arrayExists(x -> rand(x) % 2 = 0, range(g)));

-- ... also when the non-deterministic function is inside a nested lambda ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'ArrayJoin'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT g, arr FROM (SELECT number AS g, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE arrayExists(x -> arrayExists(y -> rand(y) % 2 = 0, range(x)), range(g)));

-- ... and is not pushed below it from an outer query ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'ArrayJoin'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT g, arr FROM (SELECT g, arr FROM (SELECT number AS g, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr) WHERE arrayExists(x -> rand(x) % 2 = 0, range(g)));

-- ... while a deterministic lambda is still moved below it.
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'ArrayJoin'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT g, arr FROM (SELECT number AS g, range(4) AS arr FROM numbers(100)) ARRAY JOIN arr WHERE arrayExists(x -> x % 2 = 0, range(g)));

