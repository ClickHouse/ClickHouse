-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

-- A rewrite-rule source template can itself be a rule-management statement
-- (`DROP RULE` / `ALTER RULE` / `CREATE RULE`). The fields that distinguish one such
-- statement from another (`rule_name`, and the nested source/result queries) live
-- outside the AST `children`, so they must be folded into the tree hash. Otherwise the
-- matcher treats every `DROP RULE` (or `ALTER RULE`, or `CREATE RULE`) as identical and
-- a guard written for one rule name silently intercepts unrelated statements.

-- DROP: a guard for `DROP RULE protected_rule` must match only that exact statement.
CREATE RULE guard_drop AS (DROP RULE protected_rule) REJECT WITH 'protected';
SET query_rules = 'guard_drop';
-- The exact statement is matched and rejected.
DROP RULE protected_rule;  -- { serverError REWRITE_RULE_REJECTION }
-- A different rule name must NOT match the guard: it reaches the interpreter and fails
-- because the rule does not exist, rather than being rejected by the guard.
DROP RULE some_other_rule; -- { serverError REWRITE_RULE_DOESNT_EXIST }
SET query_rules = '';

-- ALTER: same exact-match requirement.
CREATE RULE guard_alter AS (ALTER RULE protected_rule AS (SELECT 1) REWRITE TO (SELECT 2)) REJECT WITH 'protected';
SET query_rules = 'guard_alter';
ALTER RULE protected_rule AS (SELECT 1) REWRITE TO (SELECT 2);  -- { serverError REWRITE_RULE_REJECTION }
ALTER RULE some_other_rule AS (SELECT 1) REWRITE TO (SELECT 2); -- { serverError REWRITE_RULE_DOESNT_EXIST }
SET query_rules = '';

-- CREATE: same exact-match requirement. A non-matching `CREATE RULE` must reach the
-- interpreter and actually create the rule instead of being rejected by the guard.
CREATE RULE guard_create AS (CREATE RULE protected_rule AS (SELECT 1) REWRITE TO (SELECT 2)) REJECT WITH 'protected';
SET query_rules = 'guard_create';
CREATE RULE protected_rule AS (SELECT 1) REWRITE TO (SELECT 2); -- { serverError REWRITE_RULE_REJECTION }
CREATE RULE unrelated_rule AS (SELECT 3) REWRITE TO (SELECT 4);
SELECT count() FROM system.query_rules WHERE name = 'unrelated_rule';
SET query_rules = '';

DROP RULE unrelated_rule;
DROP RULE guard_create;
DROP RULE guard_alter;
DROP RULE guard_drop;
