-- `optimizeTree` enforces `force_optimize_projection_name` against the single-node plan, and only
-- afterwards does the automatic parallel replicas optimization get a chance to replace that plan. The
-- candidate it offers is built with projections disabled entirely, so adopting it would answer a query
-- that demanded a projection with a plan that uses none. Check that the optimization declines instead.

DROP TABLE IF EXISTS t_autopr_forced_projection;

CREATE TABLE t_autopr_forced_projection (key UInt64, val UInt64,
    PROJECTION p_by_val (SELECT key % 997 AS g, count() GROUP BY key % 997))
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_autopr_forced_projection SELECT number, number FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
-- `clickhouse-test` randomizes `optimize_use_projections`, and both forcing settings are gated on it
-- (`QueryPlanOptimizationSettings.cpp`: `force_projection_name` is left empty when projections are
-- off). With it randomized to 0 nothing is forced, so there would be no skip to observe.
SET optimize_use_projections = 1;
SET force_optimize_projection_name = 'p_by_val';

-- Twice: the first run would only collect statistics, so a single run could not tell a query the
-- optimization declined from one it had not finished considering yet.
SELECT key % 997 AS g, count() FROM t_autopr_forced_projection GROUP BY g ORDER BY g LIMIT 5
FORMAT Null SETTINGS log_comment = '05227_autopr_forced_projection_name';
SELECT key % 997 AS g, count() FROM t_autopr_forced_projection GROUP BY g ORDER BY g LIMIT 5
FORMAT Null SETTINGS log_comment = '05227_autopr_forced_projection_name';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, force_optimize_projection_name = '';

SYSTEM FLUSH LOGS query_log;

SELECT has(projections, concat(currentDatabase(), '.t_autopr_forced_projection.p_by_val')) AS projection_used,
       ProfileEvents['AutoParallelReplicasSkippedDueToSettings'] = 1 AS declined_for_the_projection,
       ProfileEvents['AutoParallelReplicasPlanBuildAttempts'] = 0 AS never_built_a_candidate,
       ProfileEvents['AutoParallelReplicasApplied'] = 0 AS not_applied
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05227_autopr_forced_projection_name')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_forced_projection;
