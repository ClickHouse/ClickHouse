DROP TABLE IF EXISTS test_table_join_1 SYNC;
DROP TABLE IF EXISTS test_table_join_2 SYNC;
DROP TABLE IF EXISTS test_table_join_3 SYNC;

CREATE TABLE test_table_join_1 (`id` UInt8, `value` String) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_table_join_1111', 'r1') ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_table_join_1 SELECT number, concat('t1_value_', number) FROM numbers(500);

CREATE TABLE test_table_join_2 (`id` UInt16, `value` String) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_table_join_21111', 'r1') ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_table_join_2 SELECT number, concat('t2_value_', number) FROM numbers(100, 500);

CREATE TABLE test_table_join_3 (`id` UInt64, `value` String) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_table_join_31111', 'r1') ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_table_join_3 SELECT number, concat('t3_value_', number) FROM numbers(500);

SELECT DISTINCT t3.id AS t3_id, t3.value AS t3_value, t1.id AS t1_id, id AS using_id, t2.value AS t2_value, t2.id AS t2_id, t1.value AS t1_value FROM test_table_join_1 AS t1 ANY INNER JOIN test_table_join_2 AS t2 USING (id) GLOBAL INNER JOIN test_table_join_3 AS t3 USING (id) ORDER BY ALL ASC SETTINGS parallel_replicas_local_plan = 0 settings receive_timeout = 10., receive_data_timeout_ms = 10000, allow_suspicious_low_cardinality_types = true, log_queries = true, table_function_remote_max_addresses = 200, max_execution_time = 10., max_memory_usage = 10000000000, send_logs_level = 'error', allow_introspection_functions = true, allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_mark_segment_size = 1, parallel_replicas_local_plan = false, parallel_replicas_for_cluster_engines = false, allow_experimental_analyzer = true;