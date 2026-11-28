-- Tags: zookeeper
-- Tag zookeeper: the test creates a Replicated database.

-- `DROP DATABASE` of a Replicated database drops materialized views whose inner table uses the `TimeSeries` engine.

SET allow_experimental_time_series_table = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/05325_replicated_database_drop_view_with_time_series_inner_table/{database}', 'shard1', 'replica1') FORMAT Null;

-- Several views, so that a correct drop order by chance is unlikely.
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts_1 ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts_2 ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts_3 ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts_4 ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts_5 ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts_6 ENGINE = TimeSeries AS SELECT 1 AS a FORMAT Null;

-- A plain `TimeSeries` table and a plain materialized view.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries FORMAT Null;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a FORMAT Null;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
