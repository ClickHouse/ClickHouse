-- https://github.com/ClickHouse/ClickHouse/issues/49472
SELECT
    concat(database, table) AS name,
    count()
FROM clusterAllReplicas(default, system.tables)
GROUP BY name
FORMAT Null;
