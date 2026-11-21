-- A query the cost model turns down must say so with `AutoParallelReplicasRejectedByCostModel`, not
-- only by the absence of every other outcome after `AutoParallelReplicasCostModelEvaluated`.
-- The constant string is not shipped, so the query reads and ships the same 8-byte keys. The read cap turns
-- such a small read down by itself; with the cap ignored (`automatic_parallel_replicas_ignore_thresholds`)
-- the cost model declines it only from 3 threads per replica on, because with fewer, reading in parallel
-- saves more than shipping the keys costs. Hence `max_threads` is pinned rather than left to the randomizer.
-- The shipped keys are priced with ZSTD(3), so the table pins that codec too: CI randomizes the default codec,
-- and an LZ4 part prices the read at about twice the shipped bytes, which makes the cost model apply.

DROP TABLE IF EXISTS t_autopr_rejected_by_cost_model;

CREATE TABLE t_autopr_rejected_by_cost_model (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS default_compression_codec = 'ZSTD(3)';

INSERT INTO t_autopr_rejected_by_cost_model SELECT number FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0, max_threads = 4,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Twice: the first run only collects statistics, the second is the one the cost model decides. It decides
-- at the boundary the first run remembered, without building the plan, and still says why it declined.
SELECT repeat('x', 1000) AS s, key FROM t_autopr_rejected_by_cost_model ORDER BY key
FORMAT Null SETTINGS log_comment = '05257_autopr_rejected_by_cost_model';
SELECT repeat('x', 1000) AS s, key FROM t_autopr_rejected_by_cost_model ORDER BY key
FORMAT Null SETTINGS log_comment = '05257_autopr_rejected_by_cost_model';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS evaluated,
       ProfileEvents['AutoParallelReplicasRejectedByCostModel'] AS rejected_by_cost_model,
       ProfileEvents['AutoParallelReplicasApplied'] AS applied,
       ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] AS decided_without_plan
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05257_autopr_rejected_by_cost_model')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_rejected_by_cost_model;
