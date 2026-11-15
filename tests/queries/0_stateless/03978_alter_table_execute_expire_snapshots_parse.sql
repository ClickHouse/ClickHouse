-- Tags: no-replicated-database

-- Parsing: expire_snapshots with positional timestamp (legacy syntax)
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')');
SELECT formatQuerySingleLine('ALTER TABLE db.t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')');

-- Parsing: expire_snapshots with named expire_before
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(expire_before = \'2024-06-01 00:00:00\')');

-- Parsing: expire_snapshots without arguments (uses default retention)
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots()');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(retention_period = \'3d\')');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(retain_last = 100)');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(snapshot_ids = [1, 2, 3])');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(dry_run = 1)');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(dry_run = \'True\')');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(retention_period = \'1h\', dry_run = 1)');
-- Positional timestamp combined with named args
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\', dry_run = 1)');

-- Parsing: other command names should parse successfully (generic EXECUTE syntax)
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE compact()');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE optimize_manifests()');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE some_future_command(\'arg1\', 42)');
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE no_args_command()');

-- Parsing: multiple EXECUTE commands in one ALTER statement
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE cmd1(), EXECUTE cmd2()');

-- Parsing: expression arguments in EXECUTE
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE cmd(1 + 2)');

-- AST clone/format roundtrip via EXPLAIN AST (exercises ASTAlterCommand::clone for execute_args)
EXPLAIN AST ALTER TABLE t EXECUTE expire_snapshots('2024-06-01 00:00:00');
EXPLAIN AST ALTER TABLE t EXECUTE expire_snapshots();
EXPLAIN AST ALTER TABLE t EXECUTE some_cmd('a', 42, 3.14);

-- Runtime: EXECUTE on MergeTree should fail with NOT_IMPLEMENTED
DROP TABLE IF EXISTS test_execute_03978;
CREATE TABLE test_execute_03978 (x UInt32) ENGINE = MergeTree ORDER BY x;
ALTER TABLE test_execute_03978 EXECUTE expire_snapshots('2024-06-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE expire_snapshots(expire_before = '2024-06-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE expire_snapshots(); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE compact(); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE test_execute_03978 EXECUTE unknown_command(); -- { serverError NOT_IMPLEMENTED }

-- Runtime: multiple EXECUTE commands in a single ALTER are rejected
ALTER TABLE test_execute_03978 EXECUTE cmd1(), EXECUTE cmd2(); -- { serverError QUERY_IS_PROHIBITED }

-- Execute commands can be combined with alters and mutations but not partition commands
ALTER TABLE test_execute_03978 ADD COLUMN y UInt32, EXECUTE cmd(); -- { serverError NOT_IMPLEMENTED }

DROP TABLE test_execute_03978;

-- Privilege hierarchy is verified by 01271_show_privileges (ALTER EXECUTE listed under ALTER TABLE)
