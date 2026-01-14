DROP TABLE IF EXISTS join_inner_table__fuzz_146_replicated;
CREATE TABLE join_inner_table__fuzz_146_replicated (`id` UUID, `key` String, `number` Int64, `value1` String, `value2` String, `time` Nullable(Int64)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/join_inner_table__fuzz_146_replicated', 'r1') ORDER BY (id, number, key) SETTINGS index_granularity = 8192;
SELECT number FROM join_inner_table__fuzz_146_replicated SETTINGS enable_analyzer = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', enable_parallel_replicas = 1, inject_random_order_for_select_without_order_by=1;
DROP TABLE join_inner_table__fuzz_146_replicated;
