-- Tags: replica, shard

-- `getMergeTreeSetting` returns a server-local value: each shard may have a different
-- config or `compatibility` value. The initiator of a distributed query must ship the
-- call to the shards instead of folding it into a literal computed from its own value.

SELECT getMergeTreeSetting('index_granularity')
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04539_get_merge_tree_setting_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function call, not a folded literal
-- (when folded, the shipped query starts with `SELECT _CAST(<initiator value>, ...`).
-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT getMergeTreeSetting(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04539_get_merge_tree_setting_distributed_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

-- The function-cache regression shape: identical calls in the outer scope and an inner
-- clusterAllReplicas scope must not share a FunctionBase, because the built base captures
-- `context->isDistributed()` (`isServerConstant` excludes it from the analyzer function cache).
SELECT count() AS groups, sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT getMergeTreeSetting('index_granularity') AS v, 0 AS x
        UNION ALL
        SELECT getMergeTreeSetting('index_granularity') AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
