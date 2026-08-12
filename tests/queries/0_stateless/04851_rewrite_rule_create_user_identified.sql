-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- `CREATE USER` / `ALTER USER` templates with an `IDENTIFIED ...` clause are accepted:
-- `ASTAuthenticationData` is audited for the exact-match invariant (its authentication type,
-- password-vs-hash flags and per-method `VALID UNTIL` are all folded into its tree hash), so a
-- template pins the exact authentication clause it spells out.

DROP USER IF EXISTS user_04851_a, user_04851_b;

CREATE RULE rule_04851_no_password AS (CREATE USER user_04851_a IDENTIFIED WITH no_password) REJECT WITH 'blocked';
SET query_rules = 'rule_04851_no_password';

-- The exact spelling from the template is rejected.
CREATE USER user_04851_a IDENTIFIED WITH no_password; -- { serverError REWRITE_RULE_REJECTION }

-- A different authentication clause — or none at all — must not match.
CREATE USER user_04851_a;
DROP USER user_04851_a;
CREATE USER user_04851_a IDENTIFIED WITH plaintext_password BY 'secret_04851';
DROP USER user_04851_a;

-- The per-method `VALID UNTIL` clause is kept outside the authentication node's `children`;
-- it still distinguishes templates.
CREATE USER user_04851_a IDENTIFIED WITH no_password VALID UNTIL '2126-01-01';
DROP USER user_04851_a;

SET query_rules = '';
DROP RULE rule_04851_no_password;

-- A template pinning a `VALID UNTIL` fires only for that exact expiration.
CREATE RULE rule_04851_valid_until AS (CREATE USER user_04851_b IDENTIFIED WITH no_password VALID UNTIL '2126-01-01') REJECT WITH 'blocked';
SET query_rules = 'rule_04851_valid_until';
CREATE USER user_04851_b IDENTIFIED WITH no_password VALID UNTIL '2126-01-01'; -- { serverError REWRITE_RULE_REJECTION }
CREATE USER user_04851_b IDENTIFIED WITH no_password VALID UNTIL '2126-02-02';
DROP USER user_04851_b;
SET query_rules = '';
DROP RULE rule_04851_valid_until;

-- A placeholder inside the per-method `VALID UNTIL` (an AST member the matcher does not
-- traverse) is rejected at DDL time.
CREATE RULE rule_04851_placeholder AS (CREATE USER user_04851_b IDENTIFIED WITH no_password VALID UNTIL {d:String}) REJECT WITH 'blocked'; -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

SELECT count() FROM system.users WHERE name IN ('user_04851_a', 'user_04851_b');
