-- Test: exercises analyzer's `analyzeAggregation` with GROUP BY GROUPING SETS (const)
-- Covers: src/Planner/PlannerExpressionAnalysis.cpp:200 — the GROUPING SETS branch
-- emplace_back of `available_columns_after_aggregation` must preserve the constant
-- column from `expression_dag_node->column` (was `nullptr` before the fix).
-- The plain GROUP BY const path is covered by `02992_analyzer_group_by_const.sql`
-- via `dumpColumnStructure('x') GROUP BY 'x'`. The GROUPING SETS branch (a SEPARATE
-- code path at line 200) was only covered by a `cityHash64` query that does not
-- visibly fail in release builds if the column is nullptr — `dumpColumnStructure`
-- gives a strong mutation guard by directly observing the Const wrapper.
SET enable_analyzer = 1;

-- GROUPING SETS path (line 200 in PlannerExpressionAnalysis.cpp)
SELECT dumpColumnStructure('x') GROUP BY GROUPING SETS (('x'));

-- ROLLUP / CUBE / WITH TOTALS share the non-GROUPING SETS branch (line 253),
-- which is already covered, but verify const-ness end-to-end for these
-- query shapes too — they did not appear with `dumpColumnStructure` in the suite.
SELECT dumpColumnStructure('x') GROUP BY ROLLUP ('x');
SELECT dumpColumnStructure('x') GROUP BY CUBE ('x');
