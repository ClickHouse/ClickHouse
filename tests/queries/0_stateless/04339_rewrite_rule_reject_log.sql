-- Tags: no-parallel
-- no-parallel: rewrite rules and their log are global server state

-- A `REJECT WITH` rule throws before `executeQuery` finishes, so previously the
-- rejection was never recorded. Verify the rejected query now appears in
-- `system.query_rules_log` (with an empty `resulting_query`, marking a rejection).

SET query_rules = 1;

CREATE RULE rule_reject_log AS
(
    SELECT 1 WHERE 1 = {p:String}
)
REJECT WITH 'rejected by rule_reject_log';

SELECT 1 WHERE 1 = 'x'; -- { serverError REWRITE_RULE_REJECTION }

SELECT countIf(has(applied_rules, 'rule_reject_log') AND resulting_query = '') >= 1
FROM system.query_rules_log;

DROP RULE rule_reject_log;
