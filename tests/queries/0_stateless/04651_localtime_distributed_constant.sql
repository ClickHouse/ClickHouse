-- Tags: replica, shard

-- `localtime` is a constant expression evaluated once on the initiator at query analysis time,
-- in the session/server time zone of the initiator (the SQL-standard `LOCALTIME`; documented as
-- equivalent to `CAST(now() AS Time)`, which behaves the same way). Unlike the server-local
-- introspection functions (`timezone`, `getServerSetting`, ...), it must keep folding on the
-- initiator of a distributed query: every shard receives the same literal, like `now()`.

-- The resolved projection is a constant (with the function kept as its source expression).
SELECT countIf(explain LIKE '%CONSTANT id%') = 1, countIf(explain LIKE '%function_name: localtime%') = 1
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT localtime())
SETTINGS enable_analyzer = 1;

-- The distributed query ships the folded literal to the shards.
SELECT localtime()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04651_localtime_distributed_constant';

SYSTEM FLUSH LOGS query_log;

-- The shard-side entries do not run in the test database, so they are anchored to the
-- initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT _CAST(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04651_localtime_distributed_constant'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

-- Syntactically identical calls fold to the same constant within one query
-- (guaranteed by the analyzer's shared function cache; excluding the function from
-- the cache would let two calls straddle a second boundary and differ).
SELECT localtime() = localtime();
