-- A MATERIALIZED CTE read more than once inside an IN subquery of a parallel replicas read with a local
-- plan: the query shipped to the other replicas reads the CTE by its temporary table name, so the CTE
-- must be materialized before it is sent to them (issue #112642). The CTE bodies sleep so that the other
-- replicas always start before the local read finishes.

SET enable_analyzer = 1, enable_materialized_cte = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 1;

DROP TABLE IF EXISTS t_05296;
CREATE TABLE t_05296 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05296 SELECT number, number FROM numbers(5);

-- Two references to one CTE.
SELECT count() FROM t_05296 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0)
    SELECT a.x FROM c AS a, c AS b);

-- A CTE read only inside the bodies of two other CTEs.
SELECT count() FROM t_05296 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0),
         d AS MATERIALIZED (SELECT x + 1 AS y FROM c),
         e AS MATERIALIZED (SELECT x + 2 AS y FROM c)
    SELECT d1.y FROM d AS d1, d AS d2, e AS e1, e AS e2);

-- UNION ALL whose branches chain two CTEs each.
SELECT count() FROM t_05296 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0),
         d AS MATERIALIZED (SELECT x + 1 AS y FROM c)
    SELECT DISTINCT a.y FROM d AS a, d AS b
    UNION ALL
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0),
         d AS MATERIALIZED (SELECT x + 2 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b);

-- The replicas receive a serialized query plan instead of the query text.
SELECT count() FROM t_05296 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0)
    SELECT a.x FROM c AS a, c AS b)
SETTINGS serialize_query_plan = 1;

-- The parallel replicas read is a subquery in FROM.
SELECT * FROM (
    SELECT count() FROM t_05296 WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0)
        SELECT a.x FROM c AS a, c AS b));

-- The parallel replicas read is in the body of another materialized CTE.
WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3) WHERE sleepEachRow(0.1) = 0),
     d AS MATERIALIZED (SELECT count() AS n FROM t_05296 WHERE v IN (SELECT a.x FROM c AS a, c AS b))
SELECT d1.n FROM d AS d1, d AS d2;

-- No query sent to another replica failed to find a CTE, including one whose error was discarded
-- because the local replica had already read everything.
SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log
WHERE event_date >= yesterday() AND NOT is_initial_query AND exception_code = 60
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query);

DROP TABLE t_05296;
