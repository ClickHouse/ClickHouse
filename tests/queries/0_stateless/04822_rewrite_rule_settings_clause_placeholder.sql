-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A placeholder used as a `SETTINGS` clause value (`SETTINGS max_threads = {n:Int}`) is stored
-- by the parser as a `Field` wrapping the AST inside `ASTSetQuery::changes`, outside
-- `IAST::children`, so the matcher can neither bind nor substitute it: the rule would be stored
-- but silently never work. Such placeholders are rejected at `CREATE RULE` / `ALTER RULE` time.

-- Source template.
CREATE RULE rule_settings_placeholder_source AS (SELECT 1 SETTINGS max_threads = {n:Int}) REWRITE TO (SELECT 2); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Result template.
CREATE RULE rule_settings_placeholder_result AS (SELECT {n:Int}) REWRITE TO (SELECT 1 SETTINGS max_threads = {n:Int}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- A standalone `SET` template is screened too.
CREATE RULE rule_settings_placeholder_set AS (SET max_threads = {n:Int}) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- `ALTER RULE` performs the same screening.
CREATE RULE rule_settings_placeholder_ok AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_settings_placeholder_ok AS (SELECT 1 SETTINGS max_threads = {n:Int}) REWRITE TO (SELECT 2); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Concrete `SETTINGS` values in a template keep working: the rule matches the exact query
-- (the setting value is folded into the tree hash) and rewrites it.
ALTER RULE rule_settings_placeholder_ok AS (SELECT 1 SETTINGS max_threads = 4) REWRITE TO (SELECT 2);
SET query_rules = 'rule_settings_placeholder_ok';
SELECT 1 SETTINGS max_threads = 4;
-- A different setting value does not match (fail-safe): the query runs unrewritten.
SELECT 1 SETTINGS max_threads = 3;
SET query_rules = '';
DROP RULE rule_settings_placeholder_ok;
