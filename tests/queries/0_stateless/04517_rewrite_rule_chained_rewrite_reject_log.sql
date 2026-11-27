-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- When `query_rules` chains a REWRITE rule followed by a REJECT rule, the REWRITE rule replaces
-- the query AST in place with its result template (here a secret-bearing `s3(...)` call) and the
-- later REJECT rule then throws. The rejected query is logged via the exception-before-start
-- path, which must record the query the user actually submitted -- not the rewritten rule output.
-- Otherwise `system.query_log.query` / `normalized_query_hash` would describe the rule's result
-- template (exposing its structure, with secrets masked) instead of the original query, diverging
-- from the success path which always logs the original query.

CREATE RULE rule_rewrite_to_secret AS
(
    SELECT 1
)
REWRITE TO
(
    SELECT * FROM s3('http://localhost:11111/test/file.tsv', 'MY_ACCESS_KEY_ID', 'MY_SECRET_ACCESS_KEY', 'TSV')
);

CREATE RULE rule_reject_rewritten AS
(
    SELECT * FROM s3('http://localhost:11111/test/file.tsv', 'MY_ACCESS_KEY_ID', 'MY_SECRET_ACCESS_KEY', 'TSV')
)
REJECT WITH 'rejected after rewrite';

SET query_rules = 'rule_rewrite_to_secret, rule_reject_rewritten';

SELECT 1; -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';
SYSTEM FLUSH LOGS query_log;

-- The rejected query is logged as the original `SELECT 1`; the rewritten rule output (the
-- `s3(...)` structure) and its raw secret must appear nowhere in the stored query text.
SELECT
    countIf(query LIKE 'SELECT 1%') AS original_query_logged,
    countIf(query LIKE '%file.tsv%' OR query LIKE '%s3(%') AS leaked_rewrite_structure,
    countIf(query LIKE '%MY_SECRET_ACCESS_KEY%') AS leaked_secret
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'ExceptionBeforeStart'
  AND has(applied_rules, 'rule_reject_rewritten');

DROP RULE rule_rewrite_to_secret;
DROP RULE rule_reject_rewritten;
