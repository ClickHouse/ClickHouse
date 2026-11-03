-- Checks that `automatic_parallel_replicas_remember_boundary = 0` makes every query build the plan with parallel
-- replicas, even one that the statistics of an earlier execution decline, so that benchmarks see the full cost of
-- the decision. The same shape as `05331`: the output is several times the read, so a single node wins.

DROP TABLE IF EXISTS t_remember_boundary_off SYNC;

-- The read volume is one input of the cost comparison, so pin the part layout that decides how many bytes
-- the statistics measure: the granule size, wide-vs-compact, and sparse serialization are all randomized by
-- the test harness otherwise.
CREATE TABLE t_remember_boundary_off (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_remember_boundary_off SELECT number, number FROM numbers(300000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1;
-- The per-replica floor skips building the plan on its own, from an upper bound on the read, so neutralise
-- it: this test is about the decision taken from measured statistics, not about that gate.
SET automatic_parallel_replicas_min_bytes_per_replica = 1;
-- Pin what the comparison reads: the thread count (`max_threads_min_free_memory_per_thread` otherwise
-- derives it from free memory), the task size that bounds the useful thread count, the block size that the
-- statistics sample, and the plan shape.
-- A higher thread count widens the margin: declining needs `output >= input * (1/T - 1/(T*N)) * N`, which
-- is `input / 8` at 16 threads against an output several times the input, so no plausible mis-measurement
-- of the sampled byte counts can cross it.
-- An aggregation that spills to disk merges on a path that collects no statistics, so the first execution
-- would leave nothing for the later ones to decide from; the harness randomizes the spill threshold.
SET max_threads = 16, max_threads_min_free_memory_per_thread = 0,
    merge_tree_min_bytes_per_task_for_remote_reading = 4096, max_block_size = 65505,
    serialize_query_plan = 0, max_rows_to_group_by = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

-- Off: the statistics left by the first execution decline the second as well, but only after building the plan.
SELECT id, sum(v), sum(v + 1), sum(v + 2), sum(v + 3), sum(v + 4), sum(v + 5), sum(v + 6), sum(v + 7)
FROM t_remember_boundary_off GROUP BY id FORMAT Null SETTINGS automatic_parallel_replicas_remember_boundary = 0, log_comment = 'rbo_1_off';
SELECT id, sum(v), sum(v + 1), sum(v + 2), sum(v + 3), sum(v + 4), sum(v + 5), sum(v + 6), sum(v + 7)
FROM t_remember_boundary_off GROUP BY id FORMAT Null SETTINGS automatic_parallel_replicas_remember_boundary = 0, log_comment = 'rbo_2_off';
-- On: the first execution remembers the boundary, so the second declines without building the plan.
SELECT id, sum(v), sum(v + 1), sum(v + 2), sum(v + 3), sum(v + 4), sum(v + 5), sum(v + 6), sum(v + 7)
FROM t_remember_boundary_off GROUP BY id FORMAT Null SETTINGS automatic_parallel_replicas_remember_boundary = 1, log_comment = 'rbo_3_on';
SELECT id, sum(v), sum(v + 1), sum(v + 2), sum(v + 3), sum(v + 4), sum(v + 5), sum(v + 6), sum(v + 7)
FROM t_remember_boundary_off GROUP BY id FORMAT Null SETTINGS automatic_parallel_replicas_remember_boundary = 1, log_comment = 'rbo_4_on';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    -- Without this, an optimization skipped altogether would pass for one that built the plan.
    ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0 AS built_the_plan,
    ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] > 0 AS decided_without_building_the_plan,
    ProfileEvents['ParallelReplicasUsedCount'] > 0 AS used_parallel_replicas
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `rbo_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'rbo_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_remember_boundary_off SYNC;
