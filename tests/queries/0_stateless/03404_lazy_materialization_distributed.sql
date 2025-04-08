SELECT * FROM numbers(10)
SETTINGS log_comment = '42809e74-efe7-412f-8326-7569039feecb' FORMAT Null;

-- make sure the optimization is enabled
set query_plan_optimize_lazy_materialization=true, query_plan_max_limit_for_lazy_materialization=10;

SYSTEM FLUSH LOGS;
SELECT
    log_comment,
    query,
    query_kind,
    type
FROM clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, system.query_log)
WHERE log_comment = '42809e74-efe7-412f-8326-7569039feecb' AND type != 'QueryStart' and current_database = currentDatabase()
ORDER BY event_time ASC
LIMIT 1;
