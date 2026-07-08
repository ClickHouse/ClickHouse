-- Regression for the analyzer-mode consistency of the `eval` table function on the view-inlining path.
-- `eval` infers the structure of the generated query in `TableFunctionEval::getActualTableStructure`
-- using a context that carries the generated query's own `SETTINGS`, and `StorageView::readImpl` executes
-- the inner query with the same analyzer mode. With `analyzer_inline_views = 1`, however, the new analyzer
-- would inline the eval view (`QueryAnalyzer::inlineViewSubqueryIfNeeded`) and re-analyze it with the outer
-- analyzer mode, ignoring the inner `enable_analyzer` setting. `inlineViewSubqueryIfNeeded` therefore skips
-- inlining when the view's inner analyzer mode differs from the outer one, so the view is read via
-- `readImpl` (which honors the inner mode) and the executed structure matches the inferred one.
--
-- Here the generated query pins `enable_analyzer = 0` under an analyzer-enabled outer query. The old
-- analyzer substitutes the alias `a` into the auto-generated name of the second column (`abs(1)`), while
-- the new analyzer keeps the alias (`abs(a)`). The structure is inferred with the old analyzer, so
-- execution must use the old analyzer as well; otherwise the inlined subquery produces an `abs(a)` column
-- and disagrees with the inferred `abs(1)` column.
-- See https://github.com/ClickHouse/ClickHouse/pull/104396.

SET allow_experimental_eval_table_function = 1;

-- Inlining is enabled and the inner query pins the old analyzer while the outer query uses the new one:
-- inlining must be skipped so the view is read with the inner analyzer mode. Must return `1 1`.
SELECT * FROM eval('SELECT 1 AS a, abs(a) SETTINGS enable_analyzer = 0') SETTINGS enable_analyzer = 1, analyzer_inline_views = 1;

-- Control: when the inner and outer analyzer modes match, the eval view is still eligible for inlining.
SELECT * FROM eval('SELECT 1 AS a, abs(a) SETTINGS enable_analyzer = 1') SETTINGS enable_analyzer = 1, analyzer_inline_views = 1;
