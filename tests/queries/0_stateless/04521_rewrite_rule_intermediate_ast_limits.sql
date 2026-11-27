-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: the AST size/depth limits must bound the query produced by EACH rule in a
-- `query_rules` chain, not only the final rewritten query. A first rule can rewrite a tiny query
-- into a large intermediate AST and a later rule can shrink it again; without a per-rule check the
-- oversized intermediate (and the matcher work over it) bypasses `max_ast_elements` /
-- `max_ast_depth`, which otherwise only see the submitted query and the final result.

SET max_ast_depth = 1000, max_ast_elements = 50000;

-- `r_grow` blows a tiny query up to 20 projection columns; `r_shrink` collapses that exact result
-- back to a single column. Both templates are created under the default (high) limits.
CREATE RULE r_grow AS (SELECT 1) REWRITE TO (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
CREATE RULE r_shrink AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) REWRITE TO (SELECT 100);

SET query_rules = 'r_grow, r_shrink';

-- With a tight element limit the submitted query (`SELECT 1`) and the final result (`SELECT 100`)
-- both fit, but the intermediate `SELECT 1, ..., 20` after `r_grow` does not, so the query is
-- rejected. Before the per-rule check this query ran and returned `100`.
SET max_ast_elements = 15;
SELECT 1; -- { serverError TOO_BIG_AST }

-- With the limit relaxed the same chain runs: `SELECT 1` -> `SELECT 1, ..., 20` -> `SELECT 100`.
SET max_ast_elements = 50000;
SELECT 1;

SET query_rules = '';
DROP RULE r_grow;
DROP RULE r_shrink;
