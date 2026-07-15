-- Tags: replica, shard

-- `authenticatedUser` reads `client_info.authenticated_user`, which is not propagated to
-- secondary queries: each shard observes its own authentication identity. The initiator of
-- a distributed query must ship the call to the shards instead of folding it into a literal
-- with its own authenticated user.

SELECT authenticatedUser()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04544_authenticated_user_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function call, not a folded literal
-- (when folded, the shipped query starts with `SELECT _CAST(<initiator value>, ...`).
-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT authenticatedUser(%') = 2
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
