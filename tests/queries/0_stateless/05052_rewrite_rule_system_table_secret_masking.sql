-- Tags: no-parallel, no-fasttest
-- no-parallel: rewrite rules are global server state
-- no-fasttest: the secret arguments of `s3` are declared by its table function, which is not built in the fast test

-- A rule template can embed table-function credentials. For a user who is allowed to read
-- `system.query_rules` (they hold a rule grant), the `rule` column must still mask those secrets,
-- the same way `system.named_collections` does.

CREATE RULE rule_system_table_secret AS
(
    SELECT * FROM s3('http://localhost:11111/test/file.tsv', 'MY_ACCESS_KEY_ID', 'MY_SECRET_ACCESS_KEY', 'TSV')
)
REWRITE TO
(
    SELECT 1
);

SELECT
    rule LIKE '%[HIDDEN]%' AS has_hidden,
    rule LIKE '%MY_SECRET_ACCESS_KEY%' AS leaked_secret
FROM system.query_rules
WHERE name = 'rule_system_table_secret';

DROP RULE rule_system_table_secret;
