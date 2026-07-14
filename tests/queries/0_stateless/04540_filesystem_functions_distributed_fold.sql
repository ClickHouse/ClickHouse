-- Tags: replica, shard

-- The zero-argument forms of `filesystemAvailable` / `filesystemCapacity` /
-- `filesystemUnreserved` return a server-local value: each shard reports its own
-- filesystem state. The initiator of a distributed query must ship the calls to the
-- shards instead of folding them into literals computed from its own disks.

SELECT filesystemAvailable(), filesystemCapacity(), filesystemUnreserved()
FROM clusterAllReplicas('test_cluster_two_shards', system.one)
FORMAT Null
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0, log_comment = '04540_filesystem_functions_distributed_fold';

SYSTEM FLUSH LOGS query_log;

-- Both shard queries must still contain the function calls, not folded literals.
-- The shipped query aliases every expression (`SELECT filesystemAvailable() AS ...`),
-- so match the first call as a prefix and require that no `_CAST(<initiator value>, ...`
-- literal replaced any of the three calls. The shard-side entries do not run in the test
-- database, so they are anchored to the initial query, which does.
SELECT count() = 2, countIf(query LIKE 'SELECT filesystemAvailable(%' AND query NOT LIKE '%_CAST(%') = 2
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
    AND initial_query_id =
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND log_comment = '04540_filesystem_functions_distributed_fold'
            AND is_initial_query
            AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

-- The function-cache regression shape: identical calls in the outer scope and an inner
-- clusterAllReplicas scope must not share a FunctionBase, because the built base captures
-- `context->isDistributed()` (`isServerConstant` excludes it from the analyzer function cache).
-- Only the row total is asserted: free space is volatile, so the number of distinct values
-- is not deterministic.
SELECT sum(x) AS total
FROM
(
    SELECT v, sum(x) AS x
    FROM
    (
        SELECT filesystemAvailable() AS v, 0 AS x
        UNION ALL
        SELECT filesystemAvailable() AS v, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY v
    )
    GROUP BY v
)
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
