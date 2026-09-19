SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree=1;

SELECT
    sum(number GLOBAL IN (
        SELECT number AS n
        FROM numbers(5)
        WHERE number GLOBAL IN (
            SELECT *
            FROM numbers(3)
        )
    ) AS res),
    sum(number * res)
FROM remote('127.0.0.2', numbers(10));
