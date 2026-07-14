-- Tags: replica, shard

-- `getServerSetting` returns a server-local value: each shard may have a different config
-- or runtime limit. The initiator of a distributed query must ship the call to the shards
-- instead of folding it into a literal computed from its own live value.

SELECT getServerSetting('max_server_memory_usage')
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04538_get_server_setting_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function call, not a folded literal
-- (when folded, the shipped query starts with `SELECT _CAST(<initiator value>, ...`).
SELECT count() = 2, countIf(query LIKE 'SELECT getServerSetting(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND log_comment = '04538_get_server_setting_distributed_fold' AND is_initial_query = 0 AND type = 'QueryFinish';

-- The function-cache regression shape: identical calls in the outer scope and an inner
-- clusterAllReplicas scope must not share a FunctionBase, because the built base captures
-- `context->isDistributed()` (`isServerConstant` excludes it from the analyzer function cache).
SELECT count() AS groups, sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT getServerSetting('max_server_memory_usage') AS v, 0 AS x
        UNION ALL
        SELECT getServerSetting('max_server_memory_usage') AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
