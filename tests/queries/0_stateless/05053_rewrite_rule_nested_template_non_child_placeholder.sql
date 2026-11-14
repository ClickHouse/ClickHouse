-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A placeholder inside a nested `CREATE RULE` / `ALTER RULE` template is unreachable by the
-- matcher, so it is rejected at DDL time (see `04504_rewrite_rule_nested_template_placeholder`).
-- The screening has to compose two dimensions, because a placeholder can hide behind an AST
-- member kept outside `IAST::children` as well: `{u:Identifier}` below lives in
-- `ASTCreateUserQuery::names`, and the `LIMIT` of a `SHOW` lives in
-- `ASTShowTablesQuery::limit_length`. Screening the nested template through `children` only would
-- accept these.

-- Nested template, placeholder in a non-`children` carrier (user names).
CREATE RULE rule_05053_outer AS (CREATE RULE rule_05053_inner AS (CREATE USER {u:Identifier}) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same, on the result side of the nested rule.
CREATE RULE rule_05053_outer AS (CREATE RULE rule_05053_inner AS (SELECT 1) REWRITE TO (CREATE USER {u:Identifier})) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same, with the nested rule DDL on the result side of the outer rule.
CREATE RULE rule_05053_outer AS (SELECT 1) REWRITE TO (CREATE RULE rule_05053_inner AS (CREATE USER {u:Identifier}) REWRITE TO (SELECT 1)); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Another non-`children` carrier: the `LIMIT` of a `SHOW TABLES`.
CREATE RULE rule_05053_outer AS (CREATE RULE rule_05053_inner AS (SHOW TABLES LIMIT {n:Int}) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- `ALTER RULE` performs the same screening.
CREATE RULE rule_05053_outer AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_05053_outer AS (CREATE RULE rule_05053_inner AS (CREATE USER {u:Identifier}) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE rule_05053_outer;

-- A nested rule template whose non-`children` members carry no placeholder is still accepted and
-- matched literally, by hash: the counterfactual for the checks above.
CREATE RULE rule_05053_meta AS (CREATE RULE rule_05053_inner AS (SHOW TABLES LIMIT 10) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 'rewritten');
SET query_rules = 'rule_05053_meta';
CREATE RULE rule_05053_inner AS (SHOW TABLES LIMIT 10) REWRITE TO (SELECT 1);
SET query_rules = '';
SELECT count() FROM system.query_rules WHERE name = 'rule_05053_inner';
DROP RULE rule_05053_meta;
