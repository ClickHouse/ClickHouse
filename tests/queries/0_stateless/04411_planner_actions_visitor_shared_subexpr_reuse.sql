-- Guards planner action-DAG reuse of shared/repeated subexpressions under the analyzer.
-- With enable_analyzer = 1, WITH-clause aliases are shared by pointer in the query tree (a DAG),
-- and PlannerActionsVisitor::visitFunction reuses an action node whose name already exists in the
-- single actions scope instead of re-traversing it. This test checks correctness (results unchanged)
-- for (a) nested alias chains referencing the previous alias multiple times and (b) identical
-- repeated subexpressions that are not aliased (common-subexpression reuse via the name key).
SET enable_analyzer = 1;

-- Query 1: nested if(...) alias chain over a non-constant value column.
WITH
    if(value = 10, 'A', if(value = 20, 'B', if(value = 30, 'C', if(value = 40, 'D', if(value = 50, 'E', 'F'))))) AS col1,
    if(col1 = 'A', 'Alpha', if(col1 = 'B', 'Beta', if(col1 = 'C', 'Gamma', if(col1 = 'D', 'Delta', if(col1 = 'E', 'Epsilon', 'Other'))))) AS col2,
    if(col2 = 'Alpha', 1, if(col2 = 'Beta', 2, if(col2 = 'Gamma', 3, if(col2 = 'Delta', 4, if(col2 = 'Epsilon', 5, 0))))) AS col3,
    if(col3 = 1, 100, if(col3 = 2, 200, if(col3 = 3, 300, if(col3 = 4, 400, if(col3 = 5, 500, 0))))) AS col4,
    if(col4 = 100, 'one', if(col4 = 200, 'two', if(col4 = 300, 'three', if(col4 = 400, 'four', if(col4 = 500, 'five', 'zero'))))) AS col5,
    if(col5 = 'one', 1.5, if(col5 = 'two', 2.5, if(col5 = 'three', 3.5, if(col5 = 'four', 4.5, if(col5 = 'five', 5.5, 0.5))))) AS col6
SELECT col1, col2, col3, col4, col5, col6
FROM (SELECT arrayJoin([toUInt64(10), 20, 30, 40, 50, 999]) AS value)
ORDER BY col1, col2, col3, col4, col5, col6;

-- Query 2: identical repeated subexpression that is not aliased (name-keyed CSE reuse).
SELECT (number * 7 % 5) AS e, (number * 7 % 5) + (number * 7 % 5)
FROM numbers(5)
ORDER BY number;
