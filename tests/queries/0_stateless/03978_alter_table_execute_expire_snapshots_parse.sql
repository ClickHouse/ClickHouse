-- Parsing: expire_snapshots with timestamp
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')');
SELECT formatQuerySingleLine('ALTER TABLE db.t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')');

-- Parsing: expire_snapshots without arguments (uses default retention)
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots()');

-- Parsing: other command names should parse successfully (generic EXECUTE syntax)
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE compact()');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE optimize_manifests()');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE some_future_command(\'arg1\', 42)');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE no_args_command()');

-- Runtime: EXECUTE on MergeTree should fail with NOT_IMPLEMENTED
DROP TABLE IF EXISTS test_execute_03978;
CREATE TABLE test_execute_03978 (x UInt32) ENGINE = MergeTree ORDER BY x;
ALTER TABLE test_execute_03978 EXECUTE expire_snapshots('2024-06-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE expire_snapshots(); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE compact(); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE unknown_command(); -- { serverError NOT_IMPLEMENTED }
DROP TABLE test_execute_03978;

-- Privilege hierarchy is verified by 01271_show_privileges (ALTER EXECUTE listed under ALTER TABLE)
