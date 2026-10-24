-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: the test creates its own `Replicated` database.

-- `CREATE TABLE` in a `Replicated` database is written to the database DDL log in Keeper and replayed by
-- every replica, possibly an older one during a rolling upgrade. Tuple-element `DEFAULT` expressions must be
-- pulled up to a column-level `DEFAULT tuple(...)` on the initiator before the query is enqueued, including
-- the inner column lists of targets (e.g. `SAMPLES INNER COLUMNS (...)` of a `TimeSeries` table), so the
-- entry never carries a `DEFAULT` inside a type, which an older replica would not parse.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET allow_experimental_time_series_table = 1;
SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/05258/{database}', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, x Tuple(a UInt8, s String DEFAULT 'Hello')) ENGINE = MergeTree ORDER BY id;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    timestamp DateTime64(3),
    value Float64,
    extra Tuple(a UInt8, s String DEFAULT 'Hello')
)
TAGS INNER COLUMNS
(
    extra Tuple(b Int64 DEFAULT -1)
);

-- Inner tables of `TimeSeries` are named `.inner_id.<kind>.<uuid>`.
SELECT if(table LIKE '.inner_id.%', splitByChar('.', table)[3], table) AS t, name, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() || '_1' AND name IN ('x', 'extra')
ORDER BY t, name;

-- The local replica normalizes the query again when it applies the entry, so the check above would pass even
-- if the raw new syntax had been enqueued. Assert on the log entries themselves.
SELECT
    countIf(value LIKE '%CREATE TABLE%') AS create_entries,
    countIf(value LIKE '%) DEFAULT tuple(%') AS normalized_entries,
    countIf(value LIKE '%String DEFAULT %' OR value LIKE '%Int64 DEFAULT %') AS raw_entries
FROM system.zookeeper
WHERE path = '/test/05258/' || {CLICKHOUSE_DATABASE_1:String} || '/log';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
