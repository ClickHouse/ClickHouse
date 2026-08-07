-- Checks the default storage settings of the inner tables of a TimeSeries table:
-- the samples table must get a size-based index granularity (adapts to the width of a sample row)
-- and mark-cache prewarm, and explicit user settings must always win over these defaults.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_04815;

SELECT '-- default TimeSeries table: samples get size-based granularity and mark-cache prewarm';
CREATE TABLE ts_04815 ENGINE = TimeSeries;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
SELECT '-- the tags and metrics tables are unaffected';
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.tags.%';
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.metrics.%';
SELECT '-- inserted data is readable';
INSERT INTO ts_04815 (metric_name, tags, time_series) VALUES ('cpu_usage', {'host': 'h1'}, [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.75)]);
SELECT count(), sum(value) FROM timeSeriesSamples(ts_04815);
DROP TABLE ts_04815;

SELECT '-- explicit samples_index_granularity: the size-based default is not applied';
CREATE TABLE ts_04815 ENGINE = TimeSeries SETTINGS samples_index_granularity = 16384;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
DROP TABLE ts_04815;

SELECT '-- explicit index_granularity in the samples engine declaration: the size-based default is not applied';
CREATE TABLE ts_04815 ENGINE = TimeSeries SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 4096;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
DROP TABLE ts_04815;

SELECT '-- explicit samples_index_granularity_bytes is applied together with the row-based cap';
CREATE TABLE ts_04815 ENGINE = TimeSeries SETTINGS samples_index_granularity_bytes = 131072;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
DROP TABLE ts_04815;

SELECT '-- explicit samples_index_granularity_bytes overrides the engine declaration';
CREATE TABLE ts_04815 ENGINE = TimeSeries SETTINGS samples_index_granularity_bytes = 131072 SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity_bytes = 65536;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
DROP TABLE ts_04815;

SELECT '-- both explicit: both are applied';
CREATE TABLE ts_04815 ENGINE = TimeSeries SETTINGS samples_index_granularity = 16384, samples_index_granularity_bytes = 131072;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
DROP TABLE ts_04815;

SELECT '-- explicit prewarm_mark_cache = 0 in the samples engine declaration wins';
CREATE TABLE ts_04815 ENGINE = TimeSeries SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS prewarm_mark_cache = 0;
SELECT extract(engine_full, 'SETTINGS.*') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.samples.%';
DROP TABLE ts_04815;
