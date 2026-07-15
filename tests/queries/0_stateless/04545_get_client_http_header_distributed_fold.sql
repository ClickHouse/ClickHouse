-- Tags: replica, shard

-- `getClientHTTPHeader` reads the headers of the current request, which are not propagated
-- to secondary queries: in a distributed query it returns a non-empty result only on the
-- initiator (as documented). The initiator must ship the call to the shards instead of
-- folding it into a literal with its own (potentially sensitive) header value.

SELECT getClientHTTPHeader('Content-Type')
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, allow_get_client_http_header = 1, log_comment = '04545_get_client_http_header_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function call, not a folded literal
-- (when folded, the shipped query starts with `SELECT _CAST(<initiator value>, ...`).
-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT getClientHTTPHeader(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04545_get_client_http_header_distributed_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );
