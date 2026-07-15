-- Tags: replica, shard

-- `getMaxTableNameLengthForDatabase` and `hasColumnInTable` inspect server-local state
-- (the local database path length and the local catalog). The initiator of a distributed
-- query must ship the calls to the shards instead of folding them into literals computed
-- from its own state.

SELECT getMaxTableNameLengthForDatabase('default')
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04542_max_table_name_length_fold';

SELECT hasColumnInTable('system', 'one', 'dummy')
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04542_has_column_in_table_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function call, not a folded literal
-- (when folded, the shipped query starts with `SELECT _CAST(<initiator value>, ...`).
-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT getMaxTableNameLengthForDatabase(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04542_max_table_name_length_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT count() = 2, countIf(query LIKE 'SELECT hasColumnInTable(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04542_has_column_in_table_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );
