-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- `ParserCreateUserQuery` accepts a query parameter both in the target user names and in
-- `RENAME TO`, but neither carrier is reachable by the matcher: `ASTCreateUserQuery::names` lives
-- outside `IAST::children`, and `RENAME TO` is flattened by the parser into the plain string
-- `ASTCreateUserQuery::new_name`. Such a placeholder can neither be bound nor substituted — the
-- rule would silently never match, or (on the result side) rename the user to the literal name
-- `{u:Identifier}` — so it is rejected at `CREATE RULE` / `ALTER RULE` time.

-- Source template, user names.
CREATE RULE rule_create_user_names_source AS (CREATE USER {u:Identifier}) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Result template, user names.
CREATE RULE rule_create_user_names_result AS (SELECT {u:String}) REWRITE TO (CREATE USER {u:Identifier}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Source template, `RENAME TO`. Debug builds reject the query one step earlier, with
-- `BAD_ARGUMENTS`: the AST round-trip check in `executeQuery` formats the flattened placeholder as
-- the quoted string `RENAME TO '{u:Identifier}'`, and re-parsing that trips the string-literal
-- screening in `parseUserName`. Either way the template is rejected.
CREATE RULE rule_create_user_rename_source AS (ALTER USER user_04837_a RENAME TO {u:Identifier}) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE, BAD_ARGUMENTS }

-- Result template, `RENAME TO`.
CREATE RULE rule_create_user_rename_result AS (SELECT {u:String}) REWRITE TO (ALTER USER user_04837_a RENAME TO {u:Identifier}); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE, BAD_ARGUMENTS }

-- `ALTER RULE` performs the same screening.
CREATE RULE rule_create_user_ok AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_create_user_ok AS (CREATE USER {u:Identifier}) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
ALTER RULE rule_create_user_ok AS (ALTER USER user_04837_a RENAME TO {u:Identifier}) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE, BAD_ARGUMENTS }
DROP RULE rule_create_user_ok;

-- Concrete user names in a template keep working, and are matched exactly: the user name is folded
-- into the tree hash, so a rule for `CREATE USER user_04837_a` must not fire for another user.
DROP USER IF EXISTS user_04837_a, user_04837_b;
CREATE RULE rule_create_user_exact AS (CREATE USER user_04837_a) REJECT WITH 'blocked';
SET query_rules = 'rule_create_user_exact';
CREATE USER user_04837_a; -- { serverError REWRITE_RULE_REJECTION }
CREATE USER user_04837_b;
SELECT count() FROM system.users WHERE name IN ('user_04837_a', 'user_04837_b');
SET query_rules = '';
DROP RULE rule_create_user_exact;
DROP USER user_04837_b;
