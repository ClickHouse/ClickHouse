-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

-- Regression test: an `{l:ExpressionList}` capture must be spliced into the parent
-- expression list of the result template, not nested as a single child. Otherwise the
-- analyzer treats the whole captured list as one entry, so a rule rewriting
-- `SELECT {l:ExpressionList}` to `SELECT {l:ExpressionList}, 3` over `SELECT 1, 2`
-- produced top-level projection children `[ASTExpressionList(1, 2), 3]` instead of
-- `[1, 2, 3]`.

SET query_rules = 1;

-- Splicing into a SELECT projection list.
CREATE RULE rule_exprlist_splice AS (SELECT {l:ExpressionList}) REWRITE TO (SELECT {l:ExpressionList}, 3);
SELECT 1, 2;
DROP RULE rule_exprlist_splice;

-- Splicing into a function-argument list: `greatest(1, 2)` must become
-- `greatest(1, 2, 100)` = 100, not `greatest((1, 2), 100)`.
CREATE RULE rule_exprlist_func AS (SELECT greatest({l:ExpressionList})) REWRITE TO (SELECT greatest({l:ExpressionList}, 100));
SELECT greatest(1, 2);
DROP RULE rule_exprlist_func;
