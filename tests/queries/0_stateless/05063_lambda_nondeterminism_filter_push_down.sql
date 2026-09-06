-- A filter holding a non-deterministic call inside a lambda body must not be pushed below the
-- aggregation: there it is evaluated once per input row instead of once per group.

-- The lambda has no captures, so it is folded into a constant `ColumnFunction` holding its body.
SELECT
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() >= 20, materialize([1])))) AS pushdown_allowed,
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 0) AS pushdown_disabled;

-- The lambda captures `g`, so it is a `FunctionCapture` node.
SELECT
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() + g >= 20, materialize([1])))) AS pushdown_allowed,
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() + g >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 0) AS pushdown_disabled;

-- Control: a bare non-deterministic call was already recognized.
SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING rowNumberInAllBlocks() >= 20);

-- The plan keeps the filter above the aggregation for a non-deterministic lambda ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'Aggregating'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() >= 20, materialize([1]))));

-- ... and still pushes a deterministic one below it.
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'Aggregating'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> x + g > 3, materialize([1]))));
