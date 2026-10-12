-- The builder of the plan with parallel replicas checks what it can before building anything. When it
-- stops there, the query is counted as skipped - by its settings or by its shape - not as a plan that
-- was built (`AutomaticParallelReplicasProbePlansBuilt`) and turned out unsuitable
-- (`AutoParallelReplicasPlanNotSuitable`), and its time is not charged as building one
-- (`AutoParallelReplicasPlanBuildMicroseconds`).

DROP TABLE IF EXISTS t_autopr_builder_skip;
DROP TABLE IF EXISTS t_autopr_builder_skip_2;

CREATE TABLE t_autopr_builder_skip (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_autopr_builder_skip SELECT number, number % 100 FROM numbers(100000);
CREATE TABLE t_autopr_builder_skip_2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_autopr_builder_skip_2 SELECT number, number % 10 FROM numbers(1000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Settings: without a local plan there is nothing to compare.
SELECT b, count() FROM t_autopr_builder_skip GROUP BY b FORMAT Null
SETTINGS parallel_replicas_local_plan = 0, log_comment = 'autopr_builder_skip_settings';

-- Shape: a FULL JOIN cannot be evaluated by parallelizing one of its sides.
SELECT l.b, r.b FROM t_autopr_builder_skip AS l FULL JOIN t_autopr_builder_skip_2 AS r ON l.a = r.a FORMAT Null
SETTINGS query_plan_optimize_join_order_randomize = 0, log_comment = 'autopr_builder_skip_shape';

-- Built: the plan is built, and its outcome is counted separately.
SELECT b, count() FROM t_autopr_builder_skip GROUP BY b FORMAT Null
SETTINGS log_comment = 'autopr_builder_skip_built';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasSkippedDueToSettings'] AS skipped_due_to_settings,
       ProfileEvents['AutoParallelReplicasPlanShapeNotSupported'] AS shape_not_supported,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasPlanNotSuitable'] AS not_suitable,
       ProfileEvents['AutoParallelReplicasPlanBuildMicroseconds'] > 0 AS build_timed
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND startsWith(log_comment, 'autopr_builder_skip_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment;

DROP TABLE t_autopr_builder_skip;
DROP TABLE t_autopr_builder_skip_2;
