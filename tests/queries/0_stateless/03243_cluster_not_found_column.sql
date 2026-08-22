SET enable_analyzer = 1;

SELECT
    c,
    count(c)
FROM
(
    SELECT
        dummy,
        count() AS c
    FROM clusterAllReplicas(test_cluster_two_shards, system.one)
    GROUP BY ALL
)
GROUP BY ALL;
