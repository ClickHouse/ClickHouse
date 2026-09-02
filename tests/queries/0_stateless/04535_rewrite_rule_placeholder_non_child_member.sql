-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

-- A `{name:Type}` placeholder that lands in an AST member kept OUTSIDE `IAST::children` is neither
-- bound nor substituted by the matcher (which follows only `children`), even though the tree hash
-- now folds those members in. Such a rule would be stored but silently never work, so it is
-- rejected at CREATE / ALTER RULE time — like a placeholder used as an alias or inside a nested
-- rule template.

-- `SHOW TABLES LIMIT {n:Int}`: the LIMIT lives in `ASTShowTablesQuery::limit_length`, not children.
CREATE RULE r_limit AS (SHOW TABLES LIMIT {n:Int}) REWRITE TO (SHOW TABLES LIMIT {n:Int}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- `SHOW TABLES WHERE ... = {x:String}`: the WHERE lives in `ASTShowTablesQuery::where_expression`.
CREATE RULE r_where AS (SHOW TABLES WHERE name = {x:String}) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The `USING` filter of a row policy lives in `ASTCreateRowPolicyQuery::filters`, not children.
CREATE RULE r_policy AS (CREATE ROW POLICY p ON t USING x = {v:Int}) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- A placeholder in the result template's non-`children` member is rejected too.
CREATE RULE r_result AS (SELECT {n:Int}) REWRITE TO (SHOW TABLES LIMIT {n:Int}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same rejection applies to ALTER RULE.
CREATE RULE r_alter AS (SELECT {x:String}) REWRITE TO (SELECT {x:String});
ALTER RULE r_alter AS (SHOW TABLES LIMIT {n:Int}) REWRITE TO (SHOW TABLES LIMIT {n:Int}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE r_alter;

-- A `SHOW` whose non-`children` members contain only literals (no placeholder) is accepted.
CREATE RULE r_ok AS (SHOW TABLES LIMIT 5) REWRITE TO (SHOW TABLES LIMIT 10);
SELECT count() FROM system.query_rules WHERE name = 'r_ok';
DROP RULE r_ok;
