-- Regression for the analyzer-mode consistency of the `eval` table function.
-- The structure of the generated query is inferred in `TableFunctionEval::getActualTableStructure`
-- using the generated query's own settings, so the inner query must also be executed with the same
-- analyzer mode in `StorageView::readImpl`. When the generated query sets `enable_analyzer = 0` under
-- an analyzer-enabled outer query, the structure is inferred with the old analyzer (a scalar subquery
-- yields a `_subquery1` column), so execution must use the old analyzer as well; otherwise the new
-- analyzer produces a `_subquery_1` column and the executed result cannot be converted to the inferred
-- structure. See https://github.com/ClickHouse/ClickHouse/pull/104396.

SET allow_experimental_eval_table_function = 1;

-- Generated `SETTINGS enable_analyzer = 0` under an analyzer-enabled outer query: must return 42.
SELECT * FROM eval('SELECT (SELECT 42) SETTINGS enable_analyzer = 0') SETTINGS enable_analyzer = 1;

-- Without an embedded analyzer-mode override the generated query follows the outer query and keeps
-- working under the new analyzer.
SELECT * FROM eval('SELECT (SELECT 43)') SETTINGS enable_analyzer = 1;
