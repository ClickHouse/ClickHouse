-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: every parallel replica reads all rows of the table behind `timeSeriesTags`, so each row
-- is returned once per replica, see https://github.com/ClickHouse/ClickHouse/issues/118130.

-- The clause `AS <other_table>` copies the settings of the other table and merges them with the `SETTINGS` clause
-- written in the query; the merge rules are covered by the unit test gtest_normalize_time_series_definition.cpp.
-- This test checks the parts which need a server: the engine inherited by `AS` without `ENGINE`,
-- and a copied setting really applying to the created table.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_src;
DROP TABLE IF EXISTS ts_derived;

CREATE TABLE ts_src ENGINE = TimeSeries
SETTINGS tags_to_columns = {'job': 'job'}, store_time_ranges = 0;

SELECT '-- `AS` without `ENGINE`: the engine is taken from `ts_src` and the settings are merged the same way';
CREATE TABLE ts_derived AS ts_src SETTINGS store_time_ranges = 1;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'ts_derived';
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_derived';
SELECT extract(create_table_query, 'TIME RANGES INNER COLUMNS \((.*?)\) TIME RANGES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_derived';

-- The `job` column comes with the copied inner columns anyway, so check that it's actually filled -
-- that needs the `tags_to_columns` setting. The database is passed explicitly because with parallel
-- replicas the query can go to a replica where the current database is different.
SELECT '-- the copied `tags_to_columns` fills the dedicated column';
INSERT INTO ts_derived (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(1, 1.)]);
SELECT metric_name, job FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_derived') ORDER BY metric_name;

DROP TABLE ts_derived;
DROP TABLE ts_src;

-- A custom inner engine keeps its own sorting key on copy. Only a generated sorting key may be dropped
-- and regenerated, and only while no explicit primary key survives: with one, nothing regenerates the
-- sorting key and MergeTree rejects the copy with "ORDER BY cannot be empty".
DROP TABLE IF EXISTS ts_custom_src;
DROP TABLE IF EXISTS ts_custom_copy;
CREATE TABLE ts_custom_src ENGINE = TimeSeries
SAMPLES ENGINE = ReplacingMergeTree(timestamp) PRIMARY KEY id ORDER BY (id, timestamp);

SELECT '-- a custom SAMPLES engine with its own primary key survives the copy';
CREATE TABLE ts_custom_copy AS ts_custom_src;
SELECT extract(create_table_query, 'SAMPLES INNER ENGINE = (.*?) SETTINGS')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_custom_copy';

DROP TABLE ts_custom_copy;
DROP TABLE ts_custom_src;

-- A declared `allow_nullable_key` survives the copy, because the user's nullable key column needs it.
-- Only the one generated for `min_time` and `max_time` in the sorting key goes away with that key.
DROP TABLE IF EXISTS ts_nullable_src;
DROP TABLE IF EXISTS ts_nullable_copy;

SELECT '-- a tags engine with arguments keeps allow_nullable_key';
CREATE TABLE ts_nullable_src ENGINE = TimeSeries SETTINGS version = 8
TAGS INNER COLUMNS (metric_name Nullable(String), v UInt64)
TAGS ENGINE = ReplacingMergeTree(v) PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_nullable_key = 1;
CREATE TABLE ts_nullable_copy AS ts_nullable_src;
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?)(?: [A-Z ]+ INNER |$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_nullable_copy';
DROP TABLE ts_nullable_copy;
DROP TABLE ts_nullable_src;

SELECT '-- a tags engine of another kind keeps allow_nullable_key';
CREATE TABLE ts_nullable_src ENGINE = TimeSeries
TAGS INNER COLUMNS (metric_name Nullable(String))
TAGS ENGINE = MergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_nullable_key = 1;
CREATE TABLE ts_nullable_copy AS ts_nullable_src;
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?)(?: [A-Z ]+ INNER |$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_nullable_copy';
DROP TABLE ts_nullable_copy;
DROP TABLE ts_nullable_src;

SELECT '-- the generated tags engine keeps allow_nullable_key';
CREATE TABLE ts_nullable_src ENGINE = TimeSeries SETTINGS version = 8
TAGS INNER COLUMNS (metric_name Nullable(String))
TAGS ENGINE = ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_nullable_key = 1;
CREATE TABLE ts_nullable_copy AS ts_nullable_src;
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?)(?: [A-Z ]+ INNER |$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_nullable_copy';
DROP TABLE ts_nullable_copy;
DROP TABLE ts_nullable_src;

SELECT '-- allow_nullable_key generated for min_time and max_time goes away with them';
CREATE TABLE ts_nullable_src ENGINE = TimeSeries SETTINGS version = 7, aggregate_min_time_and_max_time = 0
TAGS INNER COLUMNS (v UInt64) TAGS ENGINE = ReplacingMergeTree(v);
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?)(?: [A-Z ]+ INNER |$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_nullable_src';
CREATE TABLE ts_nullable_copy AS ts_nullable_src;
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?)(?: [A-Z ]+ INNER |$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_nullable_copy';
DROP TABLE ts_nullable_copy;
DROP TABLE ts_nullable_src;
