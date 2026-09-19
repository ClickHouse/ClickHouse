-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The matcher declares a template subtree an exact match of a query subtree when their
-- `getTreeHash(true)` values are equal, which is only sound for AST classes whose whole
-- semantics are folded into that hash. A source template containing any other class is
-- rejected at DDL time (fail closed) instead of being stored and silently over-matching:
-- e.g. `ASTCreateQuery` keeps `IF NOT EXISTS` outside both `children` and its hash, so a rule
-- for `CREATE TABLE t ...` would also fire on `CREATE TABLE IF NOT EXISTS t ...`.

-- `CREATE TABLE` (`ASTCreateQuery`): not audited.
CREATE RULE rule_unaudited_create AS (CREATE TABLE t (x Int32) ENGINE = Memory) REJECT WITH 'blocked'; -- { serverError BAD_ARGUMENTS }

-- `ALTER TABLE` (`ASTAlterQuery`): not audited.
CREATE RULE rule_unaudited_alter AS (ALTER TABLE t DROP COLUMN x) REJECT WITH 'blocked'; -- { serverError BAD_ARGUMENTS }

-- `OPTIMIZE TABLE` (`ASTOptimizeQuery`): not audited (`ON CLUSTER` and `DEDUPLICATE BY` are
-- invisible to its hash).
CREATE RULE rule_unaudited_optimize AS (OPTIMIZE TABLE t FINAL) REJECT WITH 'blocked'; -- { serverError BAD_ARGUMENTS }

-- The unaudited class is rejected wherever it appears in the template, not only at the top:
-- here inside the source template of a nested `CREATE RULE` (the outer template's hash covers
-- the nested templates too).
CREATE RULE rule_unaudited_nested AS (CREATE RULE inner_rule AS (OPTIMIZE TABLE t FINAL) REJECT WITH 'no') REJECT WITH 'blocked'; -- { serverError BAD_ARGUMENTS }

-- `ALTER RULE` performs the same screening.
CREATE RULE rule_unaudited_ok AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_unaudited_ok AS (OPTIMIZE TABLE t FINAL) REWRITE TO (SELECT 2); -- { serverError BAD_ARGUMENTS }

-- Audited statements are unaffected.
ALTER RULE rule_unaudited_ok AS (SELECT 3) REWRITE TO (SELECT 4);
SET query_rules = 'rule_unaudited_ok';
SELECT 3;
SET query_rules = '';
DROP RULE rule_unaudited_ok;
