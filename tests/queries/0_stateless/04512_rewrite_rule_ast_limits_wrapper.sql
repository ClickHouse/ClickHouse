-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The source/result templates of a CREATE RULE live outside `IAST::children`, so the generic
-- `max_ast_depth` / `max_ast_elements` walk does not reach them. When the CREATE RULE is itself
-- wrapped (e.g. `EXPLAIN AST CREATE RULE ...`), the rule interpreter never runs, so the limit
-- check must be part of the generic pre-execution AST gate to catch an oversized template even
-- via the wrapper (before access checks / interpreter dispatch).

SET max_ast_depth = 0;
SET max_ast_elements = 5;

-- A CREATE RULE with an oversized result template, wrapped in EXPLAIN AST, is rejected by the
-- limit gate before the wrapper is interpreted.
EXPLAIN AST CREATE RULE rule_wrapped_limits AS (SELECT 1) REWRITE TO (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10); -- { serverError TOO_BIG_AST }

SET max_ast_depth = 1000, max_ast_elements = 50000;

-- The rejected statement did not create a rule (EXPLAIN AST does not persist one anyway).
SELECT count() FROM system.query_rules WHERE name = 'rule_wrapped_limits';
