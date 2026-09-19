-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The per-authentication-method `GRANTS (...)` limit lives in `ASTAuthenticationData::grants`,
-- an `AccessRightsElements` kept outside `children`, yet the formatter emits it. It is folded
-- into the tree hash, so a rule template pins the exact limit it spells out instead of firing
-- on every `IDENTIFIED` clause that only shares the authentication method.

DROP USER IF EXISTS user_05230;

CREATE RULE rule_05230_grants AS (CREATE USER user_05230 IDENTIFIED WITH no_password GRANTS (SELECT ON db_05230.t)) REJECT WITH 'blocked_05230';
SET query_rules = 'rule_05230_grants';

-- The exact spelling from the template is rejected.
CREATE USER user_05230 IDENTIFIED WITH no_password GRANTS (SELECT ON db_05230.t); -- { serverError REWRITE_RULE_REJECTION }

-- The same authentication method without the clause is a different statement.
CREATE USER user_05230 IDENTIFIED WITH no_password;
DROP USER user_05230;

-- A different privilege in the clause is a different statement.
CREATE USER user_05230 IDENTIFIED WITH no_password GRANTS (INSERT ON db_05230.t);
DROP USER user_05230;

-- A different object in the clause is a different statement.
CREATE USER user_05230 IDENTIFIED WITH no_password GRANTS (SELECT ON db_05230.other);
DROP USER user_05230;

SET query_rules = '';
DROP RULE rule_05230_grants;

-- A clause granting nothing is still a clause: `GRANTS (USAGE ON *.*)` is formatted, while an
-- absent clause is not, so a presence bit - not the rendered text alone - separates the two.
CREATE RULE rule_05230_usage AS (CREATE USER user_05230 IDENTIFIED WITH no_password GRANTS (USAGE ON *.*)) REJECT WITH 'blocked_05230';
SET query_rules = 'rule_05230_usage';

CREATE USER user_05230 IDENTIFIED WITH no_password GRANTS (USAGE ON *.*); -- { serverError REWRITE_RULE_REJECTION }

CREATE USER user_05230 IDENTIFIED WITH no_password;
DROP USER user_05230;

SET query_rules = '';
DROP RULE rule_05230_usage;

SELECT count() FROM system.users WHERE name = 'user_05230';
