-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: rule matching compared `getTreeHash(true)` and `children` only. Some query types
-- the rule templates accept keep semantic fields outside both — e.g. `ASTShowTablesQuery` holds
-- `LIKE` and its flags outside `children` and outside the hash, so `SHOW TABLES LIKE 'a'` hashed
-- identically to `SHOW TABLES LIKE 'b'`. A rule template for one `SHOW` therefore over-matched
-- (and could reject or rewrite) an unrelated `SHOW`. The matcher now verifies hash-equal subtrees
-- by their formatted form, so only the intended query matches.

CREATE RULE rule_show_overmatch AS (SHOW TABLES LIKE 'rewrite_rule_show_blocked') REJECT WITH 'blocked';

SET query_rules = 'rule_show_overmatch';

-- The exact query the rule targets is still rejected.
SHOW TABLES LIKE 'rewrite_rule_show_blocked'; -- { serverError REWRITE_RULE_REJECTION }

-- A `SHOW TABLES` with a different `LIKE` must NOT be rejected (it previously was). It matches no
-- table in this database, so it returns nothing.
SHOW TABLES LIKE 'rewrite_rule_show_allowed';

SET query_rules = '';
DROP RULE rule_show_overmatch;
