-- Tags: zookeeper

SET distributed_ddl_output_mode = 'none';
SET allow_experimental_time_series_table = 1;
SET ignore_drop_queries_probability = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05293', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x Int64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
CREATE OR REPLACE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

-- Only the inner table of the current view is left in the database metadata in Keeper.
SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata' AND name LIKE '%inner_id%';
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv SYNC;
SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata' AND name LIKE '%inner_id%';

-- The same for the inner tables of a `TimeSeries` table, also as the inner table of a view: the metadata in Keeper lists exactly the tables of the database.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries;
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts ENGINE = TimeSeries AS SELECT 1 AS a;
CREATE OR REPLACE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts ENGINE = TimeSeries AS SELECT 1 AS a;
SELECT (SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata')
    = (SELECT count() FROM system.tables WHERE database = currentDatabase() || '_1');

-- `DROP DATABASE` of a `Replicated` database can fail on a view with a `TimeSeries` inner table, so the view is dropped first.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv_ts SYNC;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
