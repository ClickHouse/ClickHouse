-- Tags: zookeeper

SET distributed_ddl_output_mode = 'none';
SET allow_experimental_time_series_table = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/' || currentDatabase() || '/05293', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x Int64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
CREATE OR REPLACE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

-- Only the inner table of the current view is left in the database metadata in Keeper.
SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata' AND name LIKE '%inner_id%';
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv SYNC;
SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata' AND name LIKE '%inner_id%';

-- The same for the inner tables of a `TimeSeries` table: the metadata in Keeper lists exactly the tables of the database.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries;
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries;
SELECT (SELECT count() FROM system.zookeeper WHERE path = '/test/' || currentDatabase() || '/05293/metadata')
    = (SELECT count() FROM system.tables WHERE database = currentDatabase() || '_1');

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
