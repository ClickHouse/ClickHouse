-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: the rule matcher treats an equal `getTreeHash(true)` as semantic equality and, for
-- hash-equal nodes, descends only through `children`. That is not a valid invariant for every query
-- type a rule template accepts. `ASTSystemQuery::getID` is the constant `"SYSTEM query"` and most of
-- its distinguishing fields (`type`, target names, ...) live outside `children`; `ASTKillQueryQuery`
-- keeps `type` and `test` outside `children` and its id too. So `SYSTEM DROP UNCOMPRESSED CACHE`
-- hashed identically to `SYSTEM DROP MARK CACHE`, and `KILL MUTATION` to `KILL QUERY` — a rule
-- template for one over-matched (and could reject or rewrite) the other. The tree hashes now fold
-- those fields in, so only the intended statement matches.

-- SYSTEM: two no-child statements that differ only in `type`.
CREATE RULE rule_system_overmatch AS (SYSTEM DROP UNCOMPRESSED CACHE) REJECT WITH 'blocked';
SET query_rules = 'rule_system_overmatch';
-- The exact statement the rule targets is still rejected.
SYSTEM DROP UNCOMPRESSED CACHE; -- { serverError REWRITE_RULE_REJECTION }
-- A different SYSTEM statement (it previously over-matched) must NOT be rejected: it just runs.
SYSTEM DROP MARK CACHE;
SELECT 'system: statement of a different type is not over-matched';
SET query_rules = '';
DROP RULE rule_system_overmatch;

-- KILL: `type` (`MUTATION` vs `QUERY`) is not part of the id or the hash.
CREATE RULE rule_kill_type_overmatch AS (KILL MUTATION WHERE 0) REJECT WITH 'blocked';
SET query_rules = 'rule_kill_type_overmatch';
KILL MUTATION WHERE 0; -- { serverError REWRITE_RULE_REJECTION }
-- A KILL of a different `type` must NOT be rejected; `WHERE 0` matches nothing, so it returns no rows.
KILL QUERY WHERE 0;
SELECT 'kill: statement of a different type is not over-matched';
SET query_rules = '';
DROP RULE rule_kill_type_overmatch;

-- KILL: the `test` flag (`TEST` mode) is not part of the id or the hash either.
CREATE RULE rule_kill_test_overmatch AS (KILL QUERY WHERE 0) REJECT WITH 'blocked';
SET query_rules = 'rule_kill_test_overmatch';
KILL QUERY WHERE 0; -- { serverError REWRITE_RULE_REJECTION }
-- The same KILL in `TEST` mode differs only in the `test` flag and must NOT be rejected.
KILL QUERY WHERE 0 TEST;
SELECT 'kill: TEST mode is not over-matched';
SET query_rules = '';
DROP RULE rule_kill_test_overmatch;
