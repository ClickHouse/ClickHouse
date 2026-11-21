-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A rewrite rule template can contain nested CREATE RULE / ALTER RULE DDL buried below a
-- wrapper (here `EXPLAIN AST`). The nested rule keeps its own source/result templates outside
-- `IAST::children`, so the generic `max_ast_depth` / `max_ast_elements` walk never reaches
-- them. The explicit rule-template limit check must descend through the wrapper to find the
-- nested rule, otherwise an oversized inner template bypasses the limits and is persisted.

SET max_ast_depth = 0;
SET max_ast_elements = 5;
CREATE RULE rule_nested_limits AS
(
    EXPLAIN AST CREATE RULE inner_rule AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) REWRITE TO (SELECT 1)
)
REWRITE TO (SELECT 1); -- { serverError TOO_BIG_AST }

-- The rejected statement must not have persisted the rule. Restore the default limits first
-- (the rule-template check treats `0` as "no limit", but `QueryNormalizer` treats
-- `max_ast_depth = 0` as a literal maximum of zero and would reject this query).
SET max_ast_depth = 1000, max_ast_elements = 50000;
SELECT count() FROM system.query_rules WHERE name = 'rule_nested_limits';
