-- Guards planner action-DAG reuse of shared/repeated subexpressions under the analyzer.
-- With enable_analyzer = 1, WITH-clause aliases are shared by pointer in the query tree (a DAG),
-- and PlannerActionsVisitor::visitFunction reuses an action node whose name already exists in the
-- single actions scope instead of re-traversing it. This test checks correctness (results unchanged)
-- for (a) nested alias chains referencing the previous alias multiple times and (b) identical
-- repeated subexpressions that are not aliased (common-subexpression reuse via the name key).
SET enable_analyzer = 1;

-- Query 1: nested if(...) alias chain over a non-constant value column. Each alias references the
-- previous one several times, so col2 reuses the shared col1 node and col3 reuses the shared col2
-- node. The chain is kept shallow on purpose: the analyzer's pass cost is exponential in the chain
-- depth and is not affected by this change, so a deep chain only makes the test slow (it timed out
-- in the TSan flaky check) without adding correctness coverage - the fast-path fires on any
-- repeated reference, regardless of depth.
WITH
    if(value = 10, 'A', if(value = 20, 'B', if(value = 30, 'C', if(value = 40, 'D', if(value = 50, 'E', 'F'))))) AS col1,
    if(col1 = 'A', 'Alpha', if(col1 = 'B', 'Beta', if(col1 = 'C', 'Gamma', if(col1 = 'D', 'Delta', if(col1 = 'E', 'Epsilon', 'Other'))))) AS col2,
    if(col2 = 'Alpha', 1, if(col2 = 'Beta', 2, if(col2 = 'Gamma', 3, if(col2 = 'Delta', 4, if(col2 = 'Epsilon', 5, 0))))) AS col3
SELECT col1, col2, col3
FROM (SELECT arrayJoin([toUInt64(10), 20, 30, 40, 50, 999]) AS value)
ORDER BY col1, col2, col3;

-- Query 2: identical repeated subexpression that is not aliased (name-keyed CSE reuse).
SELECT (number * 7 % 5) AS e, (number * 7 % 5) + (number * 7 % 5)
FROM numbers(5)
ORDER BY number;
