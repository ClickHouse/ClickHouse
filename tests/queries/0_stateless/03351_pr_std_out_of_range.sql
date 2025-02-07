SELECT count(1) AS num
FROM system.query_log AS a
INNER JOIN system.processes AS b ON (type = 'QueryStart') AND (dateDiff(toNullable('second'), event_time, now()) > 5) AND (current_database = currentDatabase())
FORMAT Null
SETTINGS max_insert_delayed_streams_for_parallel_write = 55, receive_timeout = 10., receive_data_timeout_ms = 10000, allow_suspicious_low_cardinality_types = true, log_queries = true, table_function_remote_max_addresses = 200, max_execution_time = 10., max_memory_usage = 10000000000, send_logs_level = 'fatal', allow_introspection_functions = true, allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = true, parallel_replicas_min_number_of_rows_per_replica = 0, allow_experimental_analyzer = true, allow_experimental_vector_similarity_index = true;
