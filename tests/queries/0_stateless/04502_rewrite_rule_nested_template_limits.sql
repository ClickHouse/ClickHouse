-- Regression test: `max_ast_elements` / `max_ast_depth` must also bound the source and
-- result templates of a rule DDL that is nested inside another rule's template. Those
-- nested templates are stored outside `IAST::children`, so the generic size/depth walk
-- does not reach them and an oversized or very deep inner template could otherwise be
-- persisted. The outer rule's own nodes stay tiny, so only a recursive check catches it.

-- Oversized nested source template is rejected by the element limit.
SET max_ast_elements = 20;
SET max_ast_depth = 0;
CREATE RULE rule_nested_outer AS
(
    CREATE RULE rule_nested_inner AS (SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12 + 13 + 14 + 15) REWRITE TO (SELECT 1)
)
REWRITE TO (SELECT 1); -- { serverError TOO_BIG_AST }

-- Too-deep nested source template is rejected by the depth limit.
SET max_ast_elements = 0;
SET max_ast_depth = 10;
CREATE RULE rule_nested_outer AS
(
    CREATE RULE rule_nested_inner AS (SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1) REWRITE TO (SELECT 1)
)
REWRITE TO (SELECT 1); -- { serverError TOO_DEEP_AST }

-- Neither rule was persisted (both failed the limit check before storage).
SELECT count() FROM system.query_rules WHERE name IN ('rule_nested_outer', 'rule_nested_inner');
