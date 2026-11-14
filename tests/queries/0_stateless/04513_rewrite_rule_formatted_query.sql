-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- When a rewrite rule changes the query, every `system.query_log` column must describe the
-- original (pre-rewrite) query. The `query` column already does; `formatted_query` (logged when
-- `log_formatted_queries = 1`) must too, so the rewritten query — which may carry result-template
-- secrets — is not surfaced in a column that is supposed to describe the original query.

CREATE RULE rule_formatted AS (SELECT 1) REWRITE TO (SELECT 2);
SET query_rules = 'rule_formatted';
SET log_formatted_queries = 1;

-- The rule rewrites `SELECT 1` to `SELECT 2`, so this returns 2.
SELECT 1;

SET query_rules = '';
SYSTEM FLUSH LOGS query_log;

-- `formatted_query` describes the original `SELECT 1`, not the rewritten `SELECT 2`.
SELECT formatted_query
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryStart'
  AND has(applied_rules, 'rule_formatted')
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP RULE rule_formatted;
