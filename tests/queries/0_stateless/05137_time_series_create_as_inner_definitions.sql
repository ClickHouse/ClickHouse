-- `CREATE TABLE ... AS <other TimeSeries table>` copies the inner columns and engines of the other table, except
-- the parts generated for the other table: they are generated again for the new table from its settings, while
-- the customized parts are copied as they are. The settings themselves are covered by 05059_time_series_create_as_keeps_settings.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS ext_data;
DROP TABLE IF EXISTS ts_src;
DROP TABLE IF EXISTS ts_copy;

SELECT '-- the other table must be a TimeSeries table';
CREATE TABLE mt (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ts_copy AS mt ENGINE = TimeSeries; -- { serverError INCORRECT_QUERY }
DROP TABLE mt;

SELECT '-- declared engines without keys get the generated keys';
CREATE TABLE ts_src ENGINE = TimeSeries
SAMPLES ENGINE = MergeTree
RECENT SAMPLES ENGINE = MergeTree
TAGS ENGINE = AggregatingMergeTree
METRICS ENGINE = ReplacingMergeTree;
SELECT extract(create_table_query, 'SAMPLES INNER ENGINE = (.*?) RECENT SAMPLES INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_src';
SELECT extract(create_table_query, 'RECENT SAMPLES INNER ENGINE = (.*?) TAGS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_src';
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?) METRICS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_src';
SELECT extract(create_table_query, 'METRICS INNER ENGINE = (.*)$')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_src';
DROP TABLE ts_src;

SELECT '-- a customized `min_time` column is not copied if the new table does not store `min_time`/`max_time`';
CREATE TABLE ts_src ENGINE = TimeSeries
TAGS INNER COLUMNS (min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))) CODEC(ZSTD(1)));
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0;
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?) METRICS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts_src;

SELECT '-- `tags_to_columns` written in the query replaces the copied one: the column of a removed tag is not copied,';
SELECT '-- the column of an added tag is generated';
CREATE TABLE ts_src ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job', 'instance': 'instance'};
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries SETTINGS tags_to_columns = {'instance': 'instance', 'region': 'region'};
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts_src;

-- The source customizes the codec of `timestamp` and adds an extra column in the samples table, sets the `id` type,
-- and adds settings to the tags engine; everything else is generated.
CREATE TABLE ts_src ENGINE = TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, ZSTD(1)), extra UInt8)
TAGS INNER COLUMNS (id UInt64)
TAGS ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0;

SELECT '-- the customized parts are copied, the generated parts follow the settings of the new table: `min_time`/`max_time`';
SELECT '-- are not aggregated, so the tags engine is ReplacingMergeTree with them in the sorting key, and keeps its settings';
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0;
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'TAGS INNER ENGINE = (.*?) METRICS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;

SELECT '-- a type declared in the query wins over the type of the other table, the other types are inherited: `value` is';
SELECT '-- Float32 as declared, `timestamp` is DateTime64(6) as in `ts_src`. The declared samples columns replace the copied ones';
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries SAMPLES INNER COLUMNS (value Float32);
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'RECENT SAMPLES INNER COLUMNS \((.*?)\) RECENT SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts_src;

SELECT '-- `id_generator` of the other table was written for its `id` type, so it is copied only if the `id` type stays the same';
CREATE TABLE ts_src ENGINE = TimeSeries SETTINGS id_generator = 'tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags))))';
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries;
SELECT create_table_query LIKE '%id_generator%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64);
SELECT create_table_query LIKE '%id_generator%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts_src;

SELECT '-- the inner columns of the other table are not copied for a target replaced with an external table, the types come';
SELECT '-- from the external table: `timestamp` is DateTime64(3) in `ts_src` and DateTime64(6) in `ext_data`';
CREATE TABLE ext_data (id UInt64, timestamp DateTime64(6), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_src ENGINE = TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(ZSTD(5)));
CREATE TABLE ts_copy AS ts_src ENGINE = TimeSeries SAMPLES ext_data;
SELECT create_table_query LIKE '%ext_data SAMPLES INNER COLUMNS%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT extract(create_table_query, 'RECENT SAMPLES INNER COLUMNS \((.*?)\) RECENT SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts_src;
DROP TABLE ext_data;
