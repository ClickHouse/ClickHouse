-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The source/result templates of a rewrite rule are stored outside `IAST::children`,
-- so the generic AST size/depth checks in `executeQuery` do not traverse them. Verify
-- they are enforced explicitly on `CREATE RULE` / `ALTER RULE`.

-- A very deep source template must be rejected by `max_ast_depth`. Nested function
-- calls build a genuinely deep tree (redundant parentheses are collapsed by the parser).
SET max_ast_depth = 10;
CREATE RULE rule_ast_limits AS
(
    SELECT abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(abs(1))))))))))))))))))))
)
REJECT WITH 'rejected'; -- { serverError TOO_DEEP_AST }

-- A very large result template must be rejected by `max_ast_elements`.
SET max_ast_depth = 0;
SET max_ast_elements = 5;
CREATE RULE rule_ast_limits AS
(
    SELECT 1
)
REWRITE TO
(
    SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
); -- { serverError TOO_BIG_AST }

-- The rule must not have been persisted by either rejected statement.
SET max_ast_depth = 0, max_ast_elements = 0;
SELECT count() FROM system.query_rules WHERE name = 'rule_ast_limits';
