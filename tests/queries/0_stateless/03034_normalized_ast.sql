-- https://github.com/ClickHouse/ClickHouse/issues/49472
SET enable_analyzer=1;
SELECT
    concat(database, table) AS name,
    count()
FROM clusterAllReplicas(test_shard_localhost, system.tables)
WHERE database=currentDatabase()
GROUP BY name
FORMAT Null;
