-- Regression test for a LOGICAL_ERROR ("Input nodes size mismatch in dag") in the join order
-- optimizer (chooseJoinOrder -> JoinExpressionActions). A correlated scalar subquery that
-- references the same outer column twice (e.g. `number + number`) is decorrelated into a join whose
-- expression DAG exposes two INPUT nodes with the same qualified name (__table3.number). The
-- JoinExpressionActions-based reconstruction maps inputs to a join side by name and requires the
-- input count to equal the two sides' column counts, so it aborted the server in debug/sanitizer
-- builds. The optimizer must detect the duplicate-named inputs up front and skip reordering; the
-- unoptimized plan handles the query (here it raises a normal, handled exception).
-- Report (STID 4752-5ad8):
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=108721&sha=bfeef0e4f26d24a5c1eef3c7389d7b689c1a5ece&name_0=PR&name_1=Stress%20test%20%28amd_msan%29

SET compatibility = '24.3';
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET group_by_use_nulls = 1;
SET query_plan_merge_expressions = 1;
SET query_plan_merge_expression_into_join = 1;
SET query_plan_optimize_join_order_limit = 16;
SET query_plan_optimize_join_order_algorithm = 'greedy';

-- Must not abort the server with a LOGICAL_ERROR during plan optimization. The unoptimized plan
-- rejects this correlated shape with a normal handled exception, and the optimizer now defers to it.
SELECT number, (SELECT number + number) AS val
FROM numbers(4)
GROUP BY number WITH ROLLUP
ORDER BY number ASC NULLS FIRST; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- Server is still alive after the guard skipped the reorder.
SELECT 1;
