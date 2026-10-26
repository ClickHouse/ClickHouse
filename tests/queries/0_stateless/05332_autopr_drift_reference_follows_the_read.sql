-- When the statistics cache already knows a node of the plan, automatic parallel replicas takes its decision
-- from those statistics without building a plan with parallel replicas. A decline taken that way records
-- nothing, so a drift check has to keep it from outliving the data it was measured against: once the read
-- has changed by more than a factor of two, the query must recollect instead of declining again.

DROP TABLE IF EXISTS t_drift_probe SYNC;
DROP TABLE IF EXISTS t_drift_build SYNC;

-- The read volume is an input of the cost comparison, so pin the part layout that decides how many bytes
-- the statistics measure; the harness randomizes all three otherwise.
CREATE TABLE t_drift_probe (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
CREATE TABLE t_drift_build (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_drift_probe SELECT number, number FROM numbers(200000);
INSERT INTO t_drift_build SELECT number, number FROM numbers(200000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1;
-- The per-replica floor skips building the plan on its own, from an upper bound on the read; this test is
-- about the decision taken from measured statistics, so neutralise it.
SET automatic_parallel_replicas_min_bytes_per_replica = 1;
-- Pin what the comparison reads: the thread count, the task size bounding the useful thread count, the
-- plan shape, and the join order, which decides which read the statistics end up naming.
-- An aggregation that spills to disk merges on a path that collects no statistics, so the first execution
-- would leave nothing for the later ones to decide from; the harness randomizes the spill threshold.
SET max_threads = 16, max_threads_min_free_memory_per_thread = 0,
    merge_tree_min_bytes_per_task_for_remote_reading = 4096, max_block_size = 65505,
    serialize_query_plan = 0, max_rows_to_group_by = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, query_plan_optimize_join_order_randomize = 0;

-- Grouping a high-cardinality key into many aggregates makes the output several times the read, so a
-- single node wins and the model declines. The margin is deliberately wide: the byte counts are sampled
-- estimates, and a thin margin lets ordinary sampling noise flip the verdict.
SELECT p.id, sum(p.v), sum(p.v + 1), sum(p.v + 2), sum(p.v + 3), sum(p.v + 4), sum(p.v + 5)
FROM t_drift_probe p JOIN t_drift_build b ON p.id = b.id GROUP BY p.id
FORMAT Null SETTINGS log_comment = 'drift_1_collects';

-- The statistics describe this shape now, so the decision is taken from them and no plan is built.
SELECT p.id, sum(p.v), sum(p.v + 1), sum(p.v + 2), sum(p.v + 3), sum(p.v + 4), sum(p.v + 5)
FROM t_drift_probe p JOIN t_drift_build b ON p.id = b.id GROUP BY p.id
FORMAT Null SETTINGS log_comment = 'drift_2_decides_from_statistics';

-- Both tables triple, so the read the statistics were measured against has drifted well past the factor of
-- two the check tolerates, whichever side of the join it is.
INSERT INTO t_drift_probe SELECT number + 200000, number FROM numbers(400000);
INSERT INTO t_drift_build SELECT number + 200000, number FROM numbers(400000);

SELECT p.id, sum(p.v), sum(p.v + 1), sum(p.v + 2), sum(p.v + 3), sum(p.v + 4), sum(p.v + 5)
FROM t_drift_probe p JOIN t_drift_build b ON p.id = b.id GROUP BY p.id
FORMAT Null SETTINGS log_comment = 'drift_3_read_has_drifted';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] > 0 AS decided_without_building_the_plan,
    -- Statistics are collected only at a boundary the plan with parallel replicas named, so recollecting after
    -- drift builds that plan rather than trusting the remembered boundary.
    ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0 AS built_the_plan
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `drift_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'drift_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_drift_probe SYNC;
DROP TABLE t_drift_build SYNC;
