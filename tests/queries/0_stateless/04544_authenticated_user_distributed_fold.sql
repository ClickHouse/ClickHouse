-- Tags: replica, shard

-- `authenticatedUser` reads `client_info.authenticated_user`, which is not propagated to
-- secondary queries (`ClientInfo::write` does not serialize it), so on a remote shard the
-- field is empty. The initiator therefore folds the call and ships its own authenticated
-- user - the session identity, the only meaningful value - to the shards. This pins that
-- deliberate behavior: if the call were shipped unfolded instead, every shard would return
-- an empty string. Revisit if the field is ever propagated to secondary queries.

SELECT authenticatedUser()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04544_authenticated_user_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must contain the folded literal (`SELECT _CAST(<initiator value>, ...`),
-- not the function call. The shard-side entries do not run in the test database, so they are
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
            AND log_comment = '04544_authenticated_user_distributed_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );
