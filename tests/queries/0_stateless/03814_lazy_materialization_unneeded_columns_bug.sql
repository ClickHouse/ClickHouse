SELECT length(thread_ids) > 0
FROM system.query_log
WHERE (current_database = currentDatabase()) AND (event_date >= (today() - 1)) AND (lower(query) LIKE '%abcd%') AND (type = 'QueryFinish')
ORDER BY query_start_time DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 1, query_plan_optimize_lazy_materialization = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1
FORMAT Null;
