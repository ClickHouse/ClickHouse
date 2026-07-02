-- Regression for the analyzer-mode consistency of the `eval` table function.
-- `eval` infers the structure of the generated query in `TableFunctionEval::getActualTableStructure`
-- using a context that carries the generated query's own `SETTINGS`, so the inner query must also be
-- executed with the same analyzer mode. `StorageView::readImpl` therefore selects the analyzer mode
-- from the view's inner context, not from the outer query.
--
-- Here the generated query pins `enable_analyzer = 0` under an analyzer-enabled outer query. The old
-- analyzer substitutes the alias `a` into the auto-generated name of the second column (`abs(1)`),
-- while the new analyzer keeps the alias (`abs(a)`). The structure is inferred with the old analyzer,
-- so execution must use the old analyzer as well; otherwise the new analyzer produces an `abs(a)`
-- column and cannot resolve the inferred `abs(1)` column, failing with `UNKNOWN_IDENTIFIER`.
-- A deterministic auto-generated name (`abs(1)`) is used on purpose: an unaliased scalar subquery
-- such as `(SELECT 42)` is named `_subquery<N>` from a global counter, so its name differs between
-- schema inference and execution even within the same analyzer and cannot be used as a view column.
-- See https://github.com/ClickHouse/ClickHouse/pull/104396.

SET allow_experimental_eval_table_function = 1;

-- The generated query pins the old analyzer while the outer query uses the new analyzer: must return `1 1`.
SELECT * FROM eval('SELECT 1 AS a, abs(a) SETTINGS enable_analyzer = 0') SETTINGS enable_analyzer = 1;

-- Control: with matching analyzer modes the same generated query keeps working.
SELECT * FROM eval('SELECT 1 AS a, abs(a) SETTINGS enable_analyzer = 0') SETTINGS enable_analyzer = 0;
