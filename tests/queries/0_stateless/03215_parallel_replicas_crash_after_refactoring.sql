-- Tags: disabled

DROP TABLE IF EXISTS t1__fuzz_5;

CREATE TABLE t1__fuzz_5
(
    `k` Int16,
    `v` Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 10;

INSERT INTO t1__fuzz_5 SELECT
    number,
    number
FROM numbers(1000);

INSERT INTO t1__fuzz_5 SELECT
    number,
    number
FROM numbers(1000, 1000);

INSERT INTO t1__fuzz_5 SELECT
    number,
    number
FROM numbers(2000, 1000);

SET receive_timeout = 10., receive_data_timeout_ms = 10000, allow_suspicious_low_cardinality_types = true, parallel_distributed_insert_select = 2, log_queries = true, table_function_remote_max_addresses = 200, max_execution_time = 10., max_memory_usage = 10000000000, log_comment = '/workspace/ch/tests/queries/0_stateless/02869_parallel_replicas_read_from_several.sql', send_logs_level = 'warning', prefer_localhost_replica = false, allow_introspection_functions = true, use_parallel_replicas = 257, max_parallel_replicas = 65535, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = -0., parallel_replicas_for_non_replicated_merge_tree = true;

SELECT max(k) IGNORE NULLS FROM t1__fuzz_5 WITH TOTALS SETTINGS use_parallel_replicas = 257, max_parallel_replicas = 65535, prefer_localhost_replica = 0, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = -0;

DROP TABLE IF EXISTS t1__fuzz_5;
