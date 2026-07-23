SET parallel_replicas_for_non_replicated_merge_tree=1;

-- https://github.com/ClickHouse/ClickHouse/issues/49559
CREATE TABLE join_inner_table__fuzz_146 (`id` UUID, `key` String, `number` Int64, `value1` String, `value2` String, `time` Nullable(Int64)) ENGINE = MergeTree ORDER BY (id, number, key);
INSERT INTO join_inner_table__fuzz_146 SELECT CAST('833c9e22-c245-4eb5-8745-117a9a1f26b1', 'UUID') AS id, CAST(rowNumberInAllBlocks(), 'String') AS key, * FROM generateRandom('number Int64, value1 String, value2 String, time Int64', 1, 10, 2) LIMIT 100;
SELECT key, value1, value2, toUInt64(min(time)) AS start_ts FROM join_inner_table__fuzz_146 GROUP BY key, value1, value2 WITH CUBE ORDER BY key ASC NULLS LAST, value2 DESC NULLS LAST LIMIT 9223372036854775806
    FORMAT Null
    SETTINGS
        max_parallel_replicas = 3,
        prefer_localhost_replica = 1,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        enable_parallel_replicas = 1,
        use_hedged_requests = 0;


-- https://github.com/ClickHouse/ClickHouse/issues/48496
CREATE TABLE t_02709__fuzz_23 (`key` Nullable(UInt8), `sign` Int8, `date` DateTime64(3)) ENGINE = CollapsingMergeTree(sign) PARTITION BY date ORDER BY key SETTINGS allow_nullable_key=1;
INSERT INTO t_02709__fuzz_23 values (1, 1, '2023-12-01 00:00:00.000');
SELECT NULL FROM t_02709__fuzz_23 FINAL
GROUP BY sign, '1023'
ORDER BY nan DESC, [0, NULL, NULL, NULL, NULL] DESC
FORMAT Null
SETTINGS
    max_parallel_replicas = 3,
    enable_parallel_replicas = 1,
    use_hedged_requests = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT _CAST(NULL, 'Nullable(Nothing)') AS `NULL`
FROM t_02709__fuzz_23 FINAL
GROUP BY
    t_02709__fuzz_23.sign,
    '1023'
ORDER BY
    nan DESC,
    _CAST([0, NULL, NULL, NULL, NULL], 'Array(Nullable(UInt8))') DESC
FORMAT Null
SETTINGS receive_timeout = 10., receive_data_timeout_ms = 10000, use_hedged_requests = 0, allow_suspicious_low_cardinality_types = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, log_queries = 1, table_function_remote_max_addresses = 200, enable_analyzer = 1;
