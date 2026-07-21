-- Tags: replica, shard

-- The role/profile introspection functions read shard-local user state: on clusters without an
-- interserver secret the shard user is not the same user as on the initiator, and even with a
-- secret only `current_roles` are propagated - the enabled/default role sets and the settings
-- profiles always come from the shard-local user object. The initiator of a distributed query
-- must ship the calls to the shards instead of folding them into literals computed from its own
-- access state.

SELECT currentRoles(), enabledRoles(), defaultRoles()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04612_roles_distributed_fold';

SELECT currentProfiles(), enabledProfiles(), defaultProfiles()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04612_profiles_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function calls, not folded literals: a folded
-- call ships as `_CAST([<initiator values>], ...)` (the projection alias keeps the function
-- name, so the check is anchored on the first call and on the absence of `_CAST`).
-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT currentRoles()%' AND query NOT LIKE '%_CAST(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04612_roles_distributed_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT count() = 2, countIf(query LIKE 'SELECT currentProfiles()%' AND query NOT LIKE '%_CAST(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04612_profiles_distributed_fold'
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
        SELECT defaultRoles() AS v, 0 AS x
        UNION ALL
        SELECT defaultRoles() AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;

SELECT count() AS groups, sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT defaultProfiles() AS v, 0 AS x
        UNION ALL
        SELECT defaultProfiles() AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
