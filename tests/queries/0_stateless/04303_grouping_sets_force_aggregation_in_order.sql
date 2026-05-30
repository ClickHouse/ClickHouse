-- Tags: no-random-settings

-- Regression test for the LOGICAL_ERROR "Trying to get name of not a column: ExpressionList"
-- (STID 2310-3e66, issue #97988) which aborted debug builds when force_aggregation_in_order=1
-- was combined with GROUPING SETS under the old analyzer.
--
-- Root cause: InterpreterSelectQuery::executeAggregation called getSortDescriptionFromGroupBy
-- which iterates GROUP BY children and calls IAST::getColumnName on each. With GROUPING SETS
-- each set is an ASTExpressionList that has no column name; the default appendColumnName
-- raised a LOGICAL_ERROR. The optimize_aggregation_in_order branch already gated this on
-- !useGroupingSetKey() but the force_aggregation_in_order branch did not, so any query that
-- mixed the two crashed.

-- Direct path: old analyzer with force_aggregation_in_order=1 and GROUPING SETS at the top level.
SET enable_analyzer = 0, force_aggregation_in_order = 1;

SELECT 1 GROUP BY GROUPING SETS ((1), (1)) ORDER BY 1;
SELECT number FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY number;

-- Indirect path: ALTER UPDATE that pushes a GROUPING SETS subquery through
-- replaceNonDeterministicToScalars, which routes the inner SELECT through the old analyzer.
DROP TABLE IF EXISTS t_04303;
CREATE TABLE t_04303 (c0 String) ENGINE = Memory;

SET enable_analyzer = 1, mutations_execute_subqueries_on_initiator = 1, force_aggregation_in_order = 1;

-- The subquery returns two rows so the scalar promotion still fails, but with a proper
-- user-facing error (INCORRECT_RESULT_OF_SCALAR_SUBQUERY) rather than a LOGICAL_ERROR abort.
ALTER TABLE t_04303 UPDATE c0 = (SELECT 1 GROUP BY GROUPING SETS ((1), (1))) WHERE TRUE; -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

DROP TABLE t_04303;
