-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The global `VALID UNTIL` clause of `CREATE USER` (the one outside an `IDENTIFIED ...` clause)
-- is kept in the AST's `children`; `ASTCreateUserQuery::clone` used to drop it, so the clone
-- stored by a rule behaved as if the template were a plain `CREATE USER` and over-matched
-- queries without the clause.

DROP USER IF EXISTS user_04871;

CREATE RULE rule_04871_valid_until AS (CREATE USER user_04871 VALID UNTIL '2127-01-01') REJECT WITH 'blocked';
SET query_rules = 'rule_04871_valid_until';

-- A plain `CREATE USER` without `VALID UNTIL` must not match the template.
CREATE USER user_04871;
DROP USER user_04871;

-- A different expiration must not match either.
CREATE USER user_04871 VALID UNTIL '2127-02-02';
DROP USER user_04871;

-- The exact spelling from the template is rejected.
CREATE USER user_04871 VALID UNTIL '2127-01-01'; -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';
DROP RULE rule_04871_valid_until;

SELECT count() FROM system.users WHERE name = 'user_04871';
