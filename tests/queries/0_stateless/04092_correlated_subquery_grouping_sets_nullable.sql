-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102273
-- With group_by_use_nulls = 1, the outer GROUP BY wraps `number` in
-- Nullable(UInt64). A correlated subquery referencing that column must be
-- planned against the Nullable-wrapped type. Without the fix the planner
-- declares `bitNot` returning UInt64 while its input column is Nullable,
-- which caused "Unexpected return type from bitNot. Expected UInt64. Got
-- Nullable(UInt64)." for the original fuzzer query.
--
-- We assert the planner's recorded return type for the bitNot inside the
-- correlated subquery. The trailing position (": N") is stripped so the
-- reference stays stable across unrelated action-graph layout changes.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET group_by_use_nulls = 1;

SELECT replaceRegexpOne(trim(explain), ' : [0-9]+$', '') AS planner_action
FROM (
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT bitNot((SELECT bitNot(number)))
    FROM numbers(1)
    GROUP BY number WITH ROLLUP
)
WHERE explain LIKE '%FUNCTION bitNot(__table3.number : 0)%';
