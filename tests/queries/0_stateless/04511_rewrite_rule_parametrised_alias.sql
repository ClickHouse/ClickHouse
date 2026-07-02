-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A query parameter used as an alias (`expr AS {name:Type}`) is stored in
-- `ASTWithAlias::parametrised_alias`, which is NOT part of `IAST::children`. The placeholder
-- validation and the matcher / substitution all walk only `children`, so such a placeholder
-- would be neither validated, bound nor substituted, silently producing a broken rule.
-- It must be rejected at CREATE RULE / ALTER RULE time.

CREATE RULE rule_alias_source AS (SELECT 1 AS {a:Identifier}) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
CREATE RULE rule_alias_result AS (SELECT {a:String}) REWRITE TO (SELECT 1 AS {a:Identifier}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- The same validation applies to ALTER RULE.
CREATE RULE rule_alias_alter AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_alias_alter AS (SELECT 1 AS {a:Identifier}) REWRITE TO (SELECT 2); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE rule_alias_alter;

-- A literal (non-parametrized) alias is fine.
CREATE RULE rule_alias_ok AS (SELECT 1 AS a) REWRITE TO (SELECT 2 AS b);
SELECT count() FROM system.query_rules WHERE name = 'rule_alias_ok';
DROP RULE rule_alias_ok;
