-- Tags: replica, shard

-- The role/profile introspection functions deliberately fold on the initiator of a distributed
-- query, shipping the initiator's access state to the shards. This keeps the family consistent
-- with `currentUser`, which reports the propagated `initial_user` on every shard: without a
-- cluster secret the shard runs the secondary query as a different user, and even with a secret
-- only `current_roles` are propagated (settings profiles never are), so executing on the shard
-- would pair the initiator's `currentUser` identity with the shard account's roles/profiles.
-- This pins that deliberate behavior; revisit if the full role/profile state is ever propagated
-- to secondary queries.

SELECT currentRoles(), enabledRoles(), defaultRoles()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04612_roles_distributed_fold';

SELECT currentProfiles(), enabledProfiles(), defaultProfiles()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04612_profiles_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must contain the folded literals (`SELECT _CAST(<initiator values>, ...`),
-- not the function calls. The shard-side entries do not run in the test database, so they are
-- anchored to the initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT _CAST(%') = 2
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

SELECT count() = 2, countIf(query LIKE 'SELECT _CAST(%') = 2
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

-- Value-level consistency: an identical call in the outer (local) scope and in an inner
-- clusterAllReplicas scope must observe the same initiator state, so grouping by the value
-- yields a single group.
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
