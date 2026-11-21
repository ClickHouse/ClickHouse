-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- `CREATE RULE` has no existence pre-check: the storage backend consults and mutates its state
-- atomically and reports a duplicate itself (a pre-check would race with a concurrent
-- `DROP RULE` on another replica). This test pins down that the duplicate error still surfaces
-- as `REWRITE_RULE_ALREADY_EXISTS` through the storage path.

DROP RULE IF EXISTS rule_05045;

CREATE RULE rule_05045 AS (SELECT 'source_05045') REJECT WITH 'blocked_05045';
CREATE RULE rule_05045 AS (SELECT 'source_05045') REJECT WITH 'blocked_05045'; -- { serverError REWRITE_RULE_ALREADY_EXISTS }
-- A different definition under the same name is also a duplicate.
CREATE RULE rule_05045 AS (SELECT 'other_05045') REJECT WITH 'other_05045'; -- { serverError REWRITE_RULE_ALREADY_EXISTS }

-- The original rule is intact and still fires.
SET query_rules = 'rule_05045';
SELECT 'source_05045'; -- { serverError REWRITE_RULE_REJECTION }
SET query_rules = '';

-- Recreating after a drop succeeds.
DROP RULE rule_05045;
CREATE RULE rule_05045 AS (SELECT 'source_05045') REJECT WITH 'blocked_05045';
DROP RULE rule_05045;

SELECT count() FROM system.query_rules WHERE name = 'rule_05045';
