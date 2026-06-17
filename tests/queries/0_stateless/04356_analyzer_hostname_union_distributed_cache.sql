-- Tags: replica, shard

-- Regression test: the analyzer must not share the FunctionBase of server-constant
-- functions (hostName, serverUUID, ...) across scopes. The FunctionBase captures
-- context->isDistributed() at construction (FunctionConstantBase), which gates
-- isSuitableForConstantFolding. Sharing it lets the outer scope's value decide
-- whether to fold the call in an inner sub-SELECT over clusterAllReplicas, producing
-- a header mismatch like:
--   Cannot find column `_CAST('<host>'_String, 'String'_String)` in source stream,
--   there are only columns: [count()]
-- See PR #86967 (cause) and the follow-up that adds isServerConstant() to the
-- exclusion list in resolveFunction.cpp.

SELECT count() AS distinct_hosts, sum(x) AS total
FROM
(
    SELECT host, sum(x) AS x
    FROM
    (
        SELECT hostName() AS host, 0 AS x
        UNION ALL
        SELECT hostName() AS host, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY host
    )
    GROUP BY host
)
SETTINGS enable_analyzer = 1;

-- Same shape but with prefer_localhost_replica = 0 — exercises the remote path
-- (the bug surfaces as NOT_FOUND_COLUMN_IN_BLOCK instead of THERE_IS_NO_COLUMN).
SELECT count() AS distinct_hosts, sum(x) AS total
FROM
(
    SELECT host, sum(x) AS x
    FROM
    (
        SELECT hostName() AS host, 0 AS x
        UNION ALL
        SELECT hostName() AS host, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY host
    )
    GROUP BY host
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;

-- Server-constant other than hostName must behave the same way.
SELECT count() AS distinct_ports, sum(x) AS total
FROM
(
    SELECT port, sum(x) AS x
    FROM
    (
        SELECT tcpPort() AS port, 0 AS x
        UNION ALL
        SELECT tcpPort() AS port, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY port
    )
    GROUP BY port
)
SETTINGS enable_analyzer = 1;
