-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Rewrite-rule matching runs AFTER query-parameter substitution, so a value supplied through a
-- prepared-statement query parameter (`{name:Type}`) is matched as the literal it became. A
-- `REJECT` rule that blocks a specific literal therefore cannot be bypassed by passing that
-- literal through a query parameter.

CREATE RULE rule_reject_param AS
(
    SELECT 1 WHERE 1 = {p:String}
)
REJECT WITH 'rejected by rule_reject_param';

SET query_rules = 'rule_reject_param';

-- Directly with the literal: rejected.
SELECT 1 WHERE 1 = 'blocked'; -- { serverError REWRITE_RULE_REJECTION }

-- Via a query parameter that substitutes to the same literal: also rejected (this case was
-- previously bypassed because matching happened before substitution).
SET param_v = 'blocked';
SELECT 1 WHERE 1 = {v:String}; -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';

DROP RULE rule_reject_param;
