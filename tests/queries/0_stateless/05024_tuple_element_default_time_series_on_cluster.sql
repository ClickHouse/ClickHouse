-- Tags: no-replicated-database
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables synchronously, and ON CLUSTER is not allowed there.

-- Regression test: with a legacy distributed DDL entry format (below `NORMALIZE_CREATE_ON_INITIATOR_VERSION`)
-- the initiator dispatches `CREATE TABLE ... ON CLUSTER` before `getColumnsDescription` runs, so tuple-element
-- `DEFAULT` expressions must be pulled up on the initiator - including the inner column lists of targets, e.g.
-- `SAMPLES INNER COLUMNS (...)` of a `TimeSeries` table, which were missed initially.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET allow_experimental_time_series_table = 1;
SET distributed_ddl_entry_format_version = 2;

DROP TABLE IF EXISTS ts_tuple_default_cluster ON CLUSTER test_shard_localhost FORMAT Null;

CREATE TABLE ts_tuple_default_cluster ON CLUSTER test_shard_localhost ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    timestamp DateTime64(3),
    value Float64,
    extra Tuple(a UInt8, s String DEFAULT 'Hello')
)
TAGS INNER COLUMNS
(
    extra Tuple(b Int64 DEFAULT -1)
)
FORMAT Null;

SELECT splitByChar('.', table)[3] AS kind, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.%' AND name = 'extra'
ORDER BY kind;

-- The single-node worker leg normalizes the query again, so the check above would pass even if the
-- initiator dispatched the raw new syntax. Assert on the DDL log entry itself: it must carry the
-- pulled-up column-level `DEFAULT tuple(...)` and no `DEFAULT` inside a type (which an older worker
-- would not parse).
SELECT
    countIf(value LIKE '%) DEFAULT tuple(%') > 0 AS dispatched_normalized,
    countIf(value LIKE '%String DEFAULT %' OR value LIKE '%Int64 DEFAULT %') AS dispatched_raw
FROM system.zookeeper
WHERE path = '/clickhouse/task_queue/ddl'
    AND name LIKE 'query-%'
    AND value LIKE '%CREATE TABLE ' || currentDatabase() || '.ts_tuple_default_cluster%';

DROP TABLE ts_tuple_default_cluster ON CLUSTER test_shard_localhost FORMAT Null;
