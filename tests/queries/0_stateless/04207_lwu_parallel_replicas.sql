DROP TABLE IF EXISTS t_lwu_parallel_replicas;

CREATE TABLE t_lwu_parallel_replicas
(
    c0 Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_parallel_replicas VALUES (1);

-- `enable_analyzer = 0` forces the legacy parallel-replica path, which is the one that crashed.
-- `automatic_parallel_replicas_mode = 0` is required so `canUseTaskBasedParallelReplicas` enters that
-- path; `clickhouse-test` otherwise randomizes it and could bypass the legacy code being tested.
UPDATE t_lwu_parallel_replicas SET c0 = 2 WHERE 1
SETTINGS enable_lightweight_update = 1,
    enable_analyzer = 0,
    enable_parallel_replicas = 1,
    parallel_replicas_only_with_analyzer = 0,
    parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    automatic_parallel_replicas_mode = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT c0 FROM t_lwu_parallel_replicas;

DROP TABLE t_lwu_parallel_replicas;
