-- The `id_type` setting keeps the type of the `id` column in the definition of a TimeSeries table when the type
-- isn't kept there otherwise: if the tags target is an external table, or if the `id_generator` setting is set.
-- `CREATE TABLE ... AS` a table with external target tables is covered by 05138_time_series_create_as_access.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ext_tags;
DROP TABLE IF EXISTS ext_tags_with_default;

SELECT '-- the `id` type of an inner tags table is declared in its columns, so the setting is not recorded';
CREATE TABLE ts ENGINE = TimeSeries;
SELECT create_table_query LIKE '%id_type%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
DROP TABLE ts;

SELECT '-- the setting declares the `id` type of the inner tables';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS id_type = 'UInt64';
SELECT extract(create_table_query, 'id_type = ''(.*?)''') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
INSERT INTO ts (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(toDateTime64(1000, 3), 1.5)]);
SELECT id = sipHash64(tags), metric_name FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts');

SELECT '-- the setting cannot be altered';
ALTER TABLE ts MODIFY SETTING id_type = 'UUID'; -- { serverError NOT_IMPLEMENTED }
DROP TABLE ts;

SELECT '-- the setting must match the type declared in the inner columns';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS id_type = 'UInt64' TAGS INNER COLUMNS (id UUID); -- { serverError BAD_TYPE_OF_FIELD }

SELECT '-- the setting is recorded next to `id_generator`: it keeps the type the expression was written for';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS id_generator = 'sipHash64(tags)' TAGS INNER COLUMNS (id UInt64);
SELECT extract(create_table_query, 'id_type = ''(.*?)''') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
DROP TABLE ts;

SELECT '-- the `id` type and the expression generating identifiers of an external tags table are recorded in the settings';
CREATE TABLE ext_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)), max_time Nullable(DateTime64(3)))
ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);
CREATE TABLE ts ENGINE = TimeSeries TAGS ext_tags;
SELECT extract(create_table_query, 'id_type = ''(.*?)'''), extract(create_table_query, 'id_generator = ''(.*?)''')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
INSERT INTO ts (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(toDateTime64(1000, 3), 1.5)]);
SELECT id = sipHash64(tags), metric_name FROM ext_tags;
DROP TABLE ts;

SELECT '-- the DEFAULT expression of the `id` column of the external tags table is recorded as the expression';
CREATE TABLE ext_tags_with_default (id UInt64 DEFAULT cityHash64(tags), metric_name LowCardinality(String), tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)), max_time Nullable(DateTime64(3)))
ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);
CREATE TABLE ts ENGINE = TimeSeries TAGS ext_tags_with_default;
SELECT extract(create_table_query, 'id_generator = ''(.*?)''') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
INSERT INTO ts (metric_name, tags, time_series) VALUES ('m1', {'job': 'j1'}, [(toDateTime64(1000, 3), 1.5)]);
SELECT id = cityHash64(tags), metric_name FROM ext_tags_with_default;
DROP TABLE ts;

SELECT '-- an explicit `id_generator` wins over the DEFAULT expression';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS id_generator = 'sipHash64(tags)' TAGS ext_tags_with_default;
SELECT extract(create_table_query, 'id_generator = ''(.*?)''') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
DROP TABLE ts;

SELECT '-- the setting must match the type of the `id` column of the external tags table';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS id_type = 'UUID' TAGS ext_tags; -- { serverError BAD_TYPE_OF_FIELD }

DROP TABLE ext_tags_with_default;
DROP TABLE ext_tags;
