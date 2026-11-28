-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed for Replicated database.

-- Tuple-element `DEFAULT` expressions must be pulled up to the column level before
-- a `CREATE` query is dispatched to the hosts of the cluster.

DROP TABLE IF EXISTS t_default_in_tuple_cluster ON CLUSTER test_shard_localhost FORMAT Null;
CREATE TABLE t_default_in_tuple_cluster ON CLUSTER test_shard_localhost
(
    id UInt8,
    c Tuple(a UInt8, s String DEFAULT 'Hello')
)
ENGINE = MergeTree ORDER BY id FORMAT Null;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_default_in_tuple_cluster' AND name = 'c';

-- The DDL log entry must carry the pulled-up column-level `DEFAULT tuple(...)` and no `DEFAULT` inside a type
-- (which an older host would not parse).
SELECT
    countIf(value LIKE '%) DEFAULT tuple(%') > 0 AS dispatched_normalized,
    countIf(value LIKE '%String DEFAULT %') AS dispatched_raw
FROM system.zookeeper
WHERE path = '/clickhouse/task_queue/ddl'
    AND name LIKE 'query-%'
    AND value LIKE '%CREATE TABLE ' || currentDatabase() || '.t_default_in_tuple_cluster%';

DROP TABLE t_default_in_tuple_cluster ON CLUSTER test_shard_localhost FORMAT Null;
