-- Tags: no-s3-storage, no-azure-blob-storage
-- no-s3-storage, no-azure-blob-storage: the test checks that there is no crash when we set parallel_replicas_single_task_marks_count_multiplier to 0 and we have some custom heuristic for
-- finding its optimal value in case if the table is stored on remote disk.

DROP TABLE IF EXISTS 03215_parallel_replicas;

CREATE TABLE 03215_parallel_replicas
(
    `k` Int16,
    `v` Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 10;

INSERT INTO 03215_parallel_replicas SELECT
    number,
    number
FROM numbers(1000);

INSERT INTO 03215_parallel_replicas SELECT
    number,
    number
FROM numbers(1000, 1000);

INSERT INTO 03215_parallel_replicas SELECT
    number,
    number
FROM numbers(2000, 1000);

SET parallel_distributed_insert_select = 2, prefer_localhost_replica = false, enable_parallel_replicas = 1, max_parallel_replicas = 65535, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = -0., parallel_replicas_for_non_replicated_merge_tree = true;
SELECT max(k) IGNORE NULLS FROM 03215_parallel_replicas WITH TOTALS SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 65535, prefer_localhost_replica = 0, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = -0;

DROP TABLE IF EXISTS 03215_parallel_replicas;
