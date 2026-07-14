-- Tags: replica, shard

-- `connectionId` reads `client_info.connection_id`, which is not propagated to secondary
-- queries: each shard observes its own value (the frontend connection id is only set by the
-- MySQL/PostgreSQL handlers on the initiator). The initiator of a distributed query must ship
-- the call to the shards instead of folding it into a literal with its own connection id.
-- The values cannot be compared here (every connection observes 0 over the native protocol),
-- so the shipped shard queries are checked instead.

SELECT connectionId()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04541_connection_id_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function call, not a folded literal
-- (when folded, the shipped query starts with `SELECT _CAST(<initiator value>, ...`).
-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT connectionId(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04541_connection_id_distributed_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

-- The outer/inner UNION ALL shape must keep working (see 04501 for the `queryID` family).
SELECT sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT connectionId() AS v, 0 AS x
        UNION ALL
        SELECT connectionId() AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
