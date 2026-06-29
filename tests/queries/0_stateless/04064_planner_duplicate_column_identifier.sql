-- Regression test: the planner could throw LOGICAL_ERROR "Column identifier is already registered"
-- when `prepareBuildQueryPlanForTableExpression` processed a table expression multiple times
-- with an empty selected-column list (e.g., from AST fuzzer stress tests).

-- Case 1: QUALIFY with aggregate causes the same table expression to be planned twice
-- with no explicit columns selected the second time.
SELECT DISTINCT countDistinct(y)
FROM (SELECT intDiv(number, 13) AS y, number + toNullable(11) AS x FROM system.numbers LIMIT 256)
QUALIFY countDistinct(x, toFixedString(materialize(toLowCardinality(toNullable('-- default'))), 10), y)
SETTINGS count_distinct_optimization = 1, enable_analyzer = 1;
