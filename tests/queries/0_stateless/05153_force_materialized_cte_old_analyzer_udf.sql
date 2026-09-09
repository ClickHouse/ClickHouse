-- On the old analyzer the guard runs twice: in `InterpreterSelectQuery` before the CTE rewrite, and in
-- `TreeRewriter::normalize` after SQL UDF expansion, where a CTE hidden in a UDF body first becomes visible.

SET enable_analyzer = 0;
SET force_materialized_cte = 1;

SELECT 'materialized CTE from a SQL UDF body is rejected';
CREATE FUNCTION f_05153 AS () -> (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b);
SELECT f_05153(); -- { serverError SUPPORT_IS_DISABLED }
DROP FUNCTION f_05153;

SELECT 'rejected before the CTE body is resolved';
-- The early check must win over the errors that resolving the CTE body would raise.
WITH c AS MATERIALIZED (SELECT x FROM no_such_table_05153) SELECT count() FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
