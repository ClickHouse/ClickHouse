-- Tags: no-parallel
-- no-parallel: rule names are global; running in parallel may collide with other tests.

SET query_rules = 1;

-- `REJECT WITH ''` is a valid (if unusual) reject rule with an empty message.
-- Regression: previously the `reject()` predicate inferred the rule kind from
-- `reject_message.empty()`, so an empty message turned the rule into a silent
-- no-op (matching queries were neither rewritten nor rejected).

CREATE RULE rule_reject_empty_create AS (SELECT 'rule_reject_empty_create_marker') REJECT WITH '';

SELECT 'rule_reject_empty_create_marker'; -- { serverError REWRITE_RULE_REJECTION }

DROP RULE rule_reject_empty_create;

-- Same regression for `ALTER RULE ... REJECT WITH ''`: the alter copy path
-- must also preserve the reject mode independently of message contents.

CREATE RULE rule_reject_empty_alter AS (SELECT 'rule_reject_empty_alter_marker') REJECT WITH 'will be replaced';

ALTER RULE rule_reject_empty_alter AS (SELECT 'rule_reject_empty_alter_marker') REJECT WITH '';

SELECT 'rule_reject_empty_alter_marker'; -- { serverError REWRITE_RULE_REJECTION }

DROP RULE rule_reject_empty_alter;
