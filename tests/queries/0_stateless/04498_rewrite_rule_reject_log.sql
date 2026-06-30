-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A `REJECT WITH` rule throws before `executeQuery` finishes, so the query is logged via the
-- exception-before-start path. Verify the rejected query is still recorded in
-- `system.query_log` with the rejecting rule listed in the `applied_rules` column.

CREATE RULE rule_reject_log AS
(
    SELECT 1 WHERE 1 = {p:String}
)
REJECT WITH 'rejected by rule_reject_log';

SET query_rules = 'rule_reject_log';

SELECT 1 WHERE 1 = 'x'; -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';

SYSTEM FLUSH LOGS query_log;

SELECT count() >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND has(applied_rules, 'rule_reject_log');

DROP RULE rule_reject_log;
