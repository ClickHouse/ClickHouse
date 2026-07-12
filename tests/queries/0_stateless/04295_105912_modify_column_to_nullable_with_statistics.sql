-- Tags: no-replicated-database, no-shared-merge-tree, no-parallel-replicas, no-parallel
-- no-parallel: the test drives a process-global failpoint
-- (mt_mutate_task_can_skip_conversion_to_nullable_force_null_column_desc). Concurrent
-- instances would race on ENABLE/DISABLE FAILPOINT, so one run's DISABLE could clear the
-- forced-null before another run's mutation observes it, and the drift event would not fire.
-- Regression test for #105912: SIGSEGV in `canSkipConversionToNullable` when
-- the metadata snapshot a `MutateTask` ended up using no longer contains a
-- column that the queued `READ_COLUMN` command targets.
--
-- The natural race needs ZooKeeper fault injection plus aggressive concurrent
-- ALTER chaos, so reproducing it deterministically goes through a dedicated
-- failpoint that forces the `tryGet` lookup to return null. The pre-fix code
-- then dereferenced the null `ColumnDescription*` at offset `0x90` (the
-- `statistics` field). With the fix, the lookup is null-guarded and the
-- mutation falls through to the normal path.

SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 2;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_105912 SYNC;

-- Pin ratio_of_defaults_for_sparse_serialization: the optimization under test only
-- applies to DEFAULT serialization (canSkipConversionToNullable bails out early for
-- sparse columns), so a randomized sparse `y` would never reach the drift branch and
-- the event would not fire.
CREATE TABLE t_105912 (x UInt8, y UInt8 STATISTICS(tdigest))
ENGINE = MergeTree ORDER BY x
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_105912 SELECT number, number FROM numbers(100);

SYSTEM ENABLE FAILPOINT mt_mutate_task_can_skip_conversion_to_nullable_force_null_column_desc;

-- READ_COLUMN mutation: x stays the same, y becomes Nullable. The failpoint
-- makes `metadata_snapshot->getColumns().tryGet("y")` look as if `y` was
-- concurrently dropped from the metadata. With the fix, the optimization is
-- skipped and the mutation proceeds through the normal rewrite path.
ALTER TABLE t_105912 MODIFY COLUMN y Nullable(UInt8);

SYSTEM DISABLE FAILPOINT mt_mutate_task_can_skip_conversion_to_nullable_force_null_column_desc;

SELECT count(), sum(x), sum(y) FROM t_105912;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_105912' AND name = 'y';

-- The drift is not an error, but it silently degrades the metadata-only
-- optimization into a full rewrite, so it must be observable: the background
-- mutation bumps the MutateTaskMetadataDriftDetected profile event, visible in
-- the part_log entry for the mutated part.
SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['MutateTaskMetadataDriftDetected'] > 0
FROM system.part_log
WHERE database = currentDatabase()
  AND table = 't_105912'
  AND event_type = 'MutatePart'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_105912 SYNC;
