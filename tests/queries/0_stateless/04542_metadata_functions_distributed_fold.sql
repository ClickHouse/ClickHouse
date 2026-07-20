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

-- Cache-shape regression for the `isServerConstant` contract. The queries above only exercise the
-- direct `isSuitableForConstantFolding` path (a single distributed scope). Here two syntactically
-- identical calls appear in different scopes — a local outer branch and an inner sub-SELECT over
-- `clusterAllReplicas` — so they share the same tree hash. `isServerConstant` must keep them out of
-- the analyzer's shared `functions_cache`: reusing the `FunctionBase` built in the local branch
-- (`is_distributed = false`, foldable) for the distributed branch folds the call on the initiator
-- and produces a header mismatch between the local plan and the shards
-- (`NOT_FOUND_COLUMN_IN_BLOCK`). Same shape as 04356 for `hostName`.

SELECT count() AS distinct_values, sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT getMaxTableNameLengthForDatabase('default') AS v, 0 AS x
        UNION ALL
        SELECT getMaxTableNameLengthForDatabase('default') AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;

SELECT count() AS distinct_values, sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT hasColumnInTable('system', 'one', 'dummy') AS v, 0 AS x
        UNION ALL
        SELECT hasColumnInTable('system', 'one', 'dummy') AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
