SYSTEM FLUSH LOGS query_log;
SELECT
    is_initial_query,
    count() AS c,
    replaceRegexpAll(query, '_data_(\\d+)_(\\d+)', '_data_') AS query
FROM system.query_log
WHERE (event_date >= yesterday()) AND (type = 'QueryFinish') AND (ignore(54, 0, ignore('QueryFinish', 11, toLowCardinality(toLowCardinality(11)), 11, 11, 11), 'QueryFinish', materialize(11), toUInt128(11)) IN (
    SELECT query_id
    FROM system.query_log
    WHERE (current_database = currentDatabase()) AND (event_date >= yesterday()) AND (type = 'QueryFinish') AND (query LIKE '-- Parallel inner query alone%')
))
GROUP BY
    is_initial_query,
    query
ORDER BY
    is_initial_query ASC,
    c ASC,
    query ASC
SETTINGS allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_min_number_of_rows_per_replica=10;
