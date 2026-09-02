-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: rewrite-rule matching runs after query-parameter substitution. A typed (non-`String`)
-- query parameter is substituted as `_CAST(<literal>, '<type>')`, not as a bare literal, so a
-- `REJECT` rule written against a literal could be bypassed by passing the same value through a
-- typed parameter such as `{p:UInt64}`. The matcher now treats that `_CAST` wrapper as the literal
-- it stands for, so the rule cannot be bypassed and still captures typed values correctly.

CREATE RULE rule_reject_typed AS (SELECT 1 WHERE 1 = 42) REJECT WITH 'rejected by rule_reject_typed';

SET query_rules = 'rule_reject_typed';

-- Directly with the literal: rejected.
SELECT 1 WHERE 1 = 42; -- { serverError REWRITE_RULE_REJECTION }

-- Via a typed (non-String) query parameter that substitutes to the same value: also rejected
-- (this was previously bypassed because the substituted value is a `_CAST` wrapper, not a literal).
SET param_blocked = 42;
SELECT 1 WHERE 1 = {blocked:UInt64}; -- { serverError REWRITE_RULE_REJECTION }

-- A different value through the same typed parameter is NOT rejected.
SET param_other = 7;
SELECT 1 WHERE 1 = {other:UInt64};

SET query_rules = '';
DROP RULE rule_reject_typed;
