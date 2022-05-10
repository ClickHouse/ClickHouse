-- https://github.com/ClickHouse/ClickHouse/issues/37045

SELECT dummy
FROM cluster(test_cluster_two_shards, system.one)
WHERE dummy IN (
    SELECT dummy
    FROM cluster(test_cluster_two_shards, system.one)
)
LIMIT 1 BY dummy
SETTINGS distributed_product_mode = 'local'
;
