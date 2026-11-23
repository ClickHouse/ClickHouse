-- Checks that automatic parallel replicas declines a query from the statistics of an earlier execution
-- without building the plan with parallel replicas at all. Building that plan is the expensive half of the
-- optimization, and on a declined query it used to be built only to be discarded.
--
-- `AutomaticParallelReplicasDecidedWithoutBuildingPlan` fires only on that path, so it separates "declined after building the
-- plan" from "declined without building it" - which is the point of the change and is otherwise invisible from SQL.

DROP TABLE IF EXISTS t_decide_before_probe SYNC;

-- The read volume is one input of the cost comparison, so pin the part layout that decides how many bytes
-- the statistics measure: the granule size, wide-vs-compact, and sparse serialization are all randomized by
-- the test harness otherwise.
CREATE TABLE t_decide_before_probe (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_decide_before_probe SELECT number, number FROM numbers(300000);

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

-- Grouping by a high-cardinality key into many aggregates makes the output several times the read, so the
-- replicas would ship far more than they save and a single node wins. The margin is deliberately wide: the
-- byte counts are sampled estimates, and a thin margin lets ordinary sampling noise flip the verdict.
--
-- First execution: nothing is cached, so the plan with parallel replicas is built to price the query.
SELECT id, sum(v), sum(v + 1), sum(v + 2), sum(v + 3), sum(v + 4), sum(v + 5), sum(v + 6), sum(v + 7)
FROM t_decide_before_probe GROUP BY id FORMAT Null SETTINGS log_comment = 'dbp_1_collects';
-- Second: the statistics describe this shape, so the decision is taken from them and no plan is built.
SELECT id, sum(v), sum(v + 1), sum(v + 2), sum(v + 3), sum(v + 4), sum(v + 5), sum(v + 6), sum(v + 7)
FROM t_decide_before_probe GROUP BY id FORMAT Null SETTINGS log_comment = 'dbp_2_decides';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] > 0 AS decided_without_building_the_plan,
    ProfileEvents['ParallelReplicasUsedCount'] > 0 AS used_parallel_replicas
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `dbp_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'dbp_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_decide_before_probe SYNC;
