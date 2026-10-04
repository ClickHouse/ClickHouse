-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A query rejected by a REJECT rule is logged via the exception-before-start path. Its `query`
-- column must still hide table-function secrets (e.g. s3 credentials) via AST masking, the same
-- as a non-rejected query would, instead of falling back to regex-only masking that leaves such
-- positional secrets visible.

CREATE RULE rule_reject_secret AS
(
    SELECT * FROM s3('http://localhost:11111/test/file.tsv', 'MY_ACCESS_KEY_ID', 'MY_SECRET_ACCESS_KEY', 'TSV')
)
REJECT WITH 'rejected';

SET query_rules = 'rule_reject_secret';

SELECT * FROM s3('http://localhost:11111/test/file.tsv', 'MY_ACCESS_KEY_ID', 'MY_SECRET_ACCESS_KEY', 'TSV'); -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';
SYSTEM FLUSH LOGS query_log;

-- The rejected query was logged with the credentials masked ([HIDDEN]); the raw secret appears
-- nowhere in the stored query text.
SELECT
    countIf(query LIKE '%[HIDDEN]%') >= 1 AS has_hidden,
    countIf(query LIKE '%MY_SECRET_ACCESS_KEY%') AS leaked_secret
FROM system.query_log
WHERE current_database = currentDatabase()
  AND has(applied_rules, 'rule_reject_secret');

DROP RULE rule_reject_secret;
