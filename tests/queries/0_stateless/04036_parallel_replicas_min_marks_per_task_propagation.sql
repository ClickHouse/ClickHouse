-- Tags: no-fasttest, no-random-settings, no-random-mergetree-settings, long
-- no-fasttest: requires s3 storage
-- no-random-settings, no-random-mergetree-settings: a lot of settings influence task sizes, so it is simpler to disable randomization completely
-- long: times out in private

-- Test that per-part `min_marks_per_task` is propagated from the initiator to follower replicas
-- via the coordinator.
--
-- The test creates two kinds of parts:
--   - "light" parts: only UInt64 columns with narrow PK range (a in [0, 100)) → tiny compressed
--     mark size → large bytes-based estimate in `calculateMinMarksPerTask`
--   - "heavy" parts: UInt64 + randomString(100) with PK starting at 1M → large compressed mark size
--     → small bytes-based estimate
--
-- The PK filter `a >= 1_000_000` eliminates all light parts on the initiator. Replicas that skip PK
-- analysis see all parts, and `getMinMarksPerTask` takes the max — picking up the large value from
-- the light parts. With the fix, the coordinator propagates the initiator's per-part values instead.

DROP TABLE IF EXISTS t_pr_task_prop;

CREATE TABLE t_pr_task_prop (a UInt64, b UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS storage_policy = 's3_cache', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_pr_task_prop;

-- Light parts: UInt64 columns only (s is empty), PK values in [0, 100) → very small compressed mark size.
-- 3 light parts × 1M rows. Each has ~122 marks.
INSERT INTO t_pr_task_prop SELECT number % 100, number, '' FROM numbers_mt(1_000_000);
INSERT INTO t_pr_task_prop SELECT number % 100 + 100_000, number, '' FROM numbers_mt(1_000_000);
INSERT INTO t_pr_task_prop SELECT number % 100 + 200_000, number, '' FROM numbers_mt(1_000_000);

-- Heavy parts: UInt64 + randomString(100) → large compressed mark size.
-- 2 heavy parts × 1M rows with a in [1M, 2M) and [1.1M, 2.1M).
INSERT INTO t_pr_task_prop SELECT number + 1_000_000, number, randomString(100) FROM numbers_mt(1_000_000);
INSERT INTO t_pr_task_prop SELECT number + 1_100_000, number, randomString(100) FROM numbers_mt(1_000_000);

-- Verify all parts are wide (needed for per-column size information in the heuristic).
SELECT throwIf(countIf(part_type != 'Wide') > 0, 'Expected all wide parts')
FROM system.parts
WHERE (database = currentDatabase()) AND (table = 't_pr_task_prop') AND active
FORMAT Null;

SET enable_analyzer = 1;

-- Lower all floors so the heuristic in `calculateMinMarksPerTask` is the effective constraint.
-- Use a small `merge_tree_min_bytes_per_task_for_remote_reading` (48Ki) so that the bytes-based estimate
-- for light parts is meaningful: 48Ki / (small avg_mark_bytes) → large `min_marks_per_task`.
SET max_threads = 2, merge_tree_min_read_task_size = 1;

SET merge_tree_min_bytes_per_task_for_remote_reading = '48Ki';
SET merge_tree_min_bytes_for_concurrent_read=0, merge_tree_min_rows_for_concurrent_read=0;
SET merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=0, merge_tree_min_rows_for_concurrent_read_for_remote_filesystem=0;

-- PK filter `a >= 1_000_000` eliminates all light parts (their PK range is [0, 100)).
-- Initiator: sees only heavy parts (~244 marks). Large `avg_mark_bytes` → small bytes-based estimate.
-- Replicas (skipping PK analysis): see all 5 parts (~610 marks) including light parts whose tiny
-- `avg_mark_bytes` inflates `min_marks_per_task` via the bytes-based heuristic.
-- With the fix, the coordinator propagates the initiator's small per-part values to replicas.
SELECT count()
FROM t_pr_task_prop
WHERE a >= 1_000_000
FORMAT Null
SETTINGS
    automatic_parallel_replicas_mode = 0,
    enable_parallel_replicas = 2,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_index_analysis_only_on_coordinator = 1,
    optimize_use_projections = 0,
    log_comment = 'pr_task_prop_04036';

SYSTEM FLUSH LOGS query_log;

-- With propagation: replicas use the initiator's small `min_marks_per_task` from heavy parts → many requests (~130).
-- Without propagation: replicas use inflated values from light parts → few large requests (~70).
SELECT throwIf(ProfileEvents['ParallelReplicasNumRequests'] < 100, 'Too few requests — min_marks_per_task may not be propagated to replicas')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'pr_task_prop_04036'
    AND type = 'QueryFinish'
SETTINGS enable_parallel_replicas = 0
FORMAT Null;

DROP TABLE t_pr_task_prop;
