-- A filter holding a non-deterministic call inside a lambda body must not be pushed below the
-- aggregation: there it is evaluated once per input row instead of once per group.
-- `rowNumberInAllBlocks() >= 20` is the oracle: above the aggregation it sees 10 groups and passes
-- nothing, below it it sees 1000 input rows and lets every group through.

-- The positive cases pin `query_plan_filter_push_down = 1` so that they exercise the optimizer regardless of the
-- session profile; the `= 0` controls show what pushing the filter down would return.
-- The route under test is the plan-level push-down of the analyzer; the legacy analyzer moves `HAVING` into
-- `WHERE` on its own (#117758), so the test is pinned to the analyzer instead of the session default.
SET enable_analyzer = 1;

-- The lambda has no captures, so it is folded into a constant `ColumnFunction` holding its body.
SELECT
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 1) AS pushdown_allowed,
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 0) AS pushdown_disabled;

-- The lambda captures `g`, so it is a `FunctionCapture` node.
SELECT
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() + g >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 1) AS pushdown_allowed,
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() + g >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 0) AS pushdown_disabled;

-- The non-deterministic call is inside a lambda nested in another lambda, and the nested one captures
-- nothing while depending on the outer lambda argument, so it stays a `ColumnFunction` among the
-- captured columns of the outer one instead of being hoisted to the top level of the filter.
SELECT
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> arrayExists(y -> rowNumberInAllBlocks() >= 20, [x]), materialize([1]))) SETTINGS query_plan_filter_push_down = 1) AS pushdown_allowed,
    (SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> arrayExists(y -> rowNumberInAllBlocks() >= 20, [x]), materialize([1]))) SETTINGS query_plan_filter_push_down = 0) AS pushdown_disabled;

-- Control: a bare non-deterministic call was already recognized.
SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING rowNumberInAllBlocks() >= 20) SETTINGS query_plan_filter_push_down = 1;

-- The plan keeps the filter above the aggregation for a non-deterministic lambda ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'Aggregating'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> rowNumberInAllBlocks() >= 20, materialize([1]))) SETTINGS query_plan_filter_push_down = 1);

-- ... also when the non-deterministic call is inside a nested lambda ...
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'Aggregating'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> arrayExists(y -> rowNumberInAllBlocks() >= 20, [x]), materialize([1]))) SETTINGS query_plan_filter_push_down = 1);

-- ... and still pushes a deterministic one below it.
SELECT arrayStringConcat(arrayFilter(x -> x IN ('Filter', 'Aggregating'), arrayMap(y -> extract(y, '([A-Za-z]+)'), groupArray(explain))), ' ')
FROM (EXPLAIN SELECT count() FROM (SELECT number % 10 AS g FROM numbers(1000) GROUP BY g HAVING arrayExists(x -> x + g > 3, materialize([1]))) SETTINGS query_plan_filter_push_down = 1);
