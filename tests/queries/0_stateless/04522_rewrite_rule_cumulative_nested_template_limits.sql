-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: `max_ast_elements` / `max_ast_depth` must bound a rule DDL CUMULATIVELY across its
-- nested `source_query` / `resulting_query` templates, not each template in isolation. A chain of
-- nested `CREATE RULE` templates can keep every individual template under the limit while the
-- aggregate AST persisted by the single outer statement is much larger or deeper. Following a
-- template edge must not reset the accounting.

-- Each individual template here (`SELECT a, b, c`, `SELECT 1`) is small enough to pass a
-- per-template check, but the three nesting levels together exceed the element limit. Under the
-- previous per-template checks this rule was persisted.
SET max_ast_depth = 0;
SET max_ast_elements = 14;
CREATE RULE rule_cumulative_outer AS
(
    CREATE RULE rule_cumulative_mid AS
    (
        CREATE RULE rule_cumulative_inner AS (SELECT a, b, c) REWRITE TO (SELECT 1)
    )
    REWRITE TO (SELECT 1)
)
REWRITE TO (SELECT 1); -- { serverError TOO_BIG_AST }

-- The depth of the nesting chain is bounded cumulatively as well.
SET max_ast_elements = 0;
SET max_ast_depth = 7;
CREATE RULE rule_cumulative_outer AS
(
    CREATE RULE rule_cumulative_mid AS
    (
        CREATE RULE rule_cumulative_inner AS (SELECT 1) REWRITE TO (SELECT 1)
    )
    REWRITE TO (SELECT 1)
)
REWRITE TO (SELECT 1); -- { serverError TOO_DEEP_AST }

-- No rule from either rejected statement was persisted. Restore the default limits first
-- (the rule-template check treats `0` as "no limit", but `QueryNormalizer` treats
-- `max_ast_depth = 0` as a literal maximum of zero and would reject this query).
SET max_ast_depth = 1000, max_ast_elements = 50000;
SELECT count() FROM system.query_rules WHERE name LIKE 'rule_cumulative_%';
