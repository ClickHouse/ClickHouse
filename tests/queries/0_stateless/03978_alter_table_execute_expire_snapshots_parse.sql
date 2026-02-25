SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')');
SELECT formatQuerySingleLine('ALTER TABLE db.t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')');
