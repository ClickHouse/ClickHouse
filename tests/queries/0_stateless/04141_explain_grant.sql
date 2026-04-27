-- Test for EXPLAIN GRANT / EXPLAIN REVOKE: simulates a GRANT/REVOKE and returns
-- the resulting `system.grants`-shaped rows for each affected grantee, without applying
-- the change. Issue: https://github.com/ClickHouse/clickhouse-private/issues/36993

DROP USER IF EXISTS test_explain_grant_u1, test_explain_grant_u2;
CREATE USER test_explain_grant_u1, test_explain_grant_u2;

-- Single privilege on a single table.
SELECT 'simple grant';
SELECT user_name, role_name, access_type, database, table, column, is_partial_revoke, grant_option
FROM (EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u1)
ORDER BY user_name, access_type, database, table, column;

-- Umbrella privilege: CREATE expands into CREATE DATABASE / TABLE / VIEW / DICTIONARY.
-- This is the canonical example from the issue.
SELECT 'umbrella grant';
SELECT user_name, access_type, database, table
FROM (EXPLAIN GRANT CREATE ON foo.* TO test_explain_grant_u1)
ORDER BY user_name, access_type;

-- WITH GRANT OPTION marks grant_option=1.
SELECT 'with grant option';
SELECT user_name, access_type, database, grant_option, is_partial_revoke
FROM (EXPLAIN GRANT SELECT ON *.* TO test_explain_grant_u1 WITH GRANT OPTION)
ORDER BY user_name;

-- Multiple grantees emit rows for each.
SELECT 'multiple grantees';
SELECT user_name, access_type, database, table
FROM (EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u1, test_explain_grant_u2)
ORDER BY user_name;

-- Partial revoke after a wildcard grant. Apply the wildcard for real, then EXPLAIN
-- the REVOKE — output must include `is_partial_revoke = 1` for the system.* rows
-- and the original wildcard rows must remain.
GRANT SELECT ON *.* TO test_explain_grant_u1;
SELECT 'partial revoke';
SELECT user_name, access_type, database, table, is_partial_revoke
FROM (EXPLAIN REVOKE SELECT ON system.* FROM test_explain_grant_u1)
ORDER BY user_name, is_partial_revoke, database;

-- The GRANT was real, but the REVOKE was only EXPLAINed. The wildcard SELECT must
-- still be visible in `system.grants`, with no partial revoke row recorded.
SELECT 'no side effect';
SELECT count() FROM system.grants
WHERE user_name = 'test_explain_grant_u1' AND is_partial_revoke = 1;

DROP USER test_explain_grant_u1, test_explain_grant_u2;
