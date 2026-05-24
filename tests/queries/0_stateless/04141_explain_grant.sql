-- Tags: no-parallel
-- ^ User names are global in `local_directory` access storage; running this
-- test concurrently with itself collides on `CREATE USER test_explain_grant_u1`.

-- Test for EXPLAIN GRANT / EXPLAIN REVOKE: simulates a GRANT/REVOKE and returns
-- the resulting `system.grants`-shaped rows for each affected grantee, without applying
-- the change. Issue: https://github.com/ClickHouse/clickhouse-private/issues/36993
--
-- Note: the inner query of `(EXPLAIN ...)` subquery wrapping is currently restricted
-- to SELECT (the analyzer assumes the inner is `ASTSelectWithUnionQuery`). Filtering /
-- ordering of EXPLAIN GRANT output via subquery is left as a follow-up; this test calls
-- EXPLAIN GRANT at top level and dumps all 9 columns of the `system.grants` schema.

DROP USER IF EXISTS test_explain_grant_u1, test_explain_grant_u2;
CREATE USER test_explain_grant_u1, test_explain_grant_u2;

-- Single privilege on a single table.
SELECT 'simple grant';
EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u1;

-- Umbrella privilege: CREATE expands into CREATE DATABASE / TABLE / VIEW / DICTIONARY.
-- This is the canonical example from the issue.
SELECT 'umbrella grant';
EXPLAIN GRANT CREATE ON foo.* TO test_explain_grant_u1;

-- WITH GRANT OPTION marks grant_option=1.
SELECT 'with grant option';
EXPLAIN GRANT SELECT ON *.* TO test_explain_grant_u1 WITH GRANT OPTION;

-- Each grantee gets its own simulated post-state. We run two single-grantee queries
-- rather than one multi-grantee query so the output order is deterministic (the
-- multi-grantee form orders by AccessControl UUID, which is randomised per-create).
SELECT 'grantee u1';
EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u1;
SELECT 'grantee u2';
EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u2;

-- Partial revoke after a wildcard grant. Apply the wildcard for real, then EXPLAIN
-- the REVOKE — output must include the surviving wildcard SELECT and a partial-revoke
-- row for `system`.
GRANT SELECT ON *.* TO test_explain_grant_u1;
SELECT 'partial revoke';
EXPLAIN REVOKE SELECT ON system.* FROM test_explain_grant_u1;

-- The GRANT was real, but the REVOKE was only EXPLAINed. The wildcard SELECT must
-- still be visible in `system.grants`, with no partial revoke row recorded.
SELECT 'no side effect';
SELECT count() FROM system.grants
WHERE user_name = 'test_explain_grant_u1' AND is_partial_revoke = 1;

-- ON CLUSTER is rejected — explanation is local-only. The rejection must fire on the
-- *original* AST, before `removeOnClusterClauseIfNeeded` would otherwise strip the
-- clause when `ignore_on_cluster_for_replicated_access_entities_queries=1` with
-- replicated access storage. We can't easily set up replicated storage in a stateless
-- test, but we still cover the basic rejection path with the default settings.
SELECT 'on cluster rejected';
EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u1 ON CLUSTER 'test_shard_localhost'; -- { serverError BAD_ARGUMENTS }
EXPLAIN GRANT SELECT ON db.t TO test_explain_grant_u1 ON CLUSTER 'test_shard_localhost'
SETTINGS ignore_on_cluster_for_replicated_access_entities_queries = 1; -- { serverError BAD_ARGUMENTS }

DROP USER test_explain_grant_u1, test_explain_grant_u2;
