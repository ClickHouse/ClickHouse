-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The tags table of a TimeSeries table with histograms keeps the kinds of samples of every time series in the column
-- `sample_kinds` (1 - float samples, 2 - histogram samples), and `timeSeriesSelector` reads only the data tables of
-- the kinds present. Without stored time ranges there is no column, and both tables are read.
-- Parallel replicas are disabled: the reads of the data tables must show up in the local plan. Every table gets a histogram
-- sample of another metric: the read of an empty table is planned as `ReadNothing`, not as `ReadFromMergeTree`.

SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 0;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_replacing;
DROP TABLE IF EXISTS ts_no_time_ranges;
DROP TABLE IF EXISTS ts_unfiltered;
DROP TABLE IF EXISTS ts_external;
DROP TABLE IF EXISTS ts_external_tags;
DROP TABLE IF EXISTS ts_external_samples;
DROP TABLE IF EXISTS ts_bad;
DROP TABLE IF EXISTS ts_custom_memory;
DROP TABLE IF EXISTS ts_custom_columns;
DROP TABLE IF EXISTS ts_custom_engine;
DROP TABLE IF EXISTS ts_bad_tags;

CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES ('floats', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('histograms', {'job': 'a'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
-- Both kinds in one row, and in two inserts.
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('mixed', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2)], [toDateTime64('2026-01-01 00:00:01', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('mixed', {'job': 'b'}, [(toDateTime64('2026-01-01 00:00:00', 3), 3)]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('mixed', {'job': 'b'}, [toDateTime64('2026-01-01 00:00:01', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
-- No samples.
INSERT INTO ts (metric_name, tags) VALUES ('empty', {'job': 'a'});

SELECT '-- the kinds stored in the tags table';
SELECT metric_name, tags['job'], groupBitOr(sample_kinds) FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts') GROUP BY ALL ORDER BY ALL;

SELECT '-- the data tables read by a selector: samples, histograms';
SELECT 'floats', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'histograms', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'histograms', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'mixed', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'mixed', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'mixed{job="b"}', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'mixed{job="b"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'floats or histograms', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, '{__name__=~"floats|histograms"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'empty', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'empty', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));

SELECT '-- the samples returned by a selector: value, number of histograms';
SELECT 'floats', value, length(histogram)
FROM timeSeriesSelector(ts, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)) ORDER BY ALL;
SELECT 'histograms', value, length(histogram)
FROM timeSeriesSelector(ts, 'histograms', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)) ORDER BY ALL;
SELECT 'mixed', value, length(histogram)
FROM timeSeriesSelector(ts, 'mixed', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)) ORDER BY ALL;
SELECT 'floats or histograms', value, length(histogram)
FROM timeSeriesSelector(ts, '{__name__=~"floats|histograms"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)) ORDER BY ALL;
SELECT 'empty', count() FROM timeSeriesSelector(ts, 'empty', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '-- PromQL over float samples only, and over histogram samples which it does not support yet';
SELECT tags, value FROM prometheusQuery(ts, 'floats', toDateTime64('2026-01-01 00:01:00', 3)) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'mixed', toDateTime64('2026-01-01 00:01:00', 3)) ORDER BY ALL; -- { serverError NOT_IMPLEMENTED }

SELECT '-- a time series without samples found without the time filter';
CREATE TABLE ts_unfiltered ENGINE = TimeSeries SETTINGS filter_by_min_time_and_max_time = 0;
INSERT INTO ts_unfiltered (metric_name, tags) VALUES ('empty', {});
SELECT 'empty', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_unfiltered, 'empty', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));

SELECT '-- without aggregation the column is a part of the sorting key';
CREATE TABLE ts_replacing ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0;
SELECT extract(create_table_query, 'TAGS INNER ENGINE = [^ ]+ PRIMARY KEY metric_name ORDER BY (\\(.*?\\))')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_replacing';
INSERT INTO ts_replacing (metric_name, tags, samples) VALUES ('floats', {}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts_replacing (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('other', {}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
SELECT 'floats', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_replacing, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));

SELECT '-- without the time ranges there is no column, both tables are read';
CREATE TABLE ts_no_time_ranges ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0;
SELECT countMatches(create_table_query, 'sample_kinds') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_no_time_ranges';
INSERT INTO ts_no_time_ranges (metric_name, tags, samples) VALUES ('floats', {}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts_no_time_ranges (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('other', {}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
SELECT 'floats', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_no_time_ranges, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));

SELECT '-- an external tags table gets the column with DEFAULT 1 for the rows written by a version without histograms';
CREATE TABLE ts_external_tags
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ts_external_samples (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
INSERT INTO ts_external_tags VALUES (1, 'old', {'__name__': 'old'}, toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:00:00', 3));
INSERT INTO ts_external_samples VALUES (1, toDateTime64('2026-01-01 00:00:00', 3), 5);
CREATE TABLE ts_external ENGINE = TimeSeries SAMPLES ts_external_samples TAGS ts_external_tags; -- { serverError THERE_IS_NO_COLUMN }
ALTER TABLE ts_external_tags ADD COLUMN sample_kinds UInt8 DEFAULT 1;
CREATE TABLE ts_external ENGINE = TimeSeries SAMPLES ts_external_samples TAGS ts_external_tags;
INSERT INTO ts_external (metric_name, tags, samples) VALUES ('floats', {}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts_external (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('other', {}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
SELECT 'floats', countIf(explain LIKE '%ReadFromMergeTree%samples%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_external, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'old', countIf(explain LIKE '%ReadFromMergeTree%samples%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_external, 'old', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'old', value, length(histogram) FROM timeSeriesSelector(ts_external, 'old', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '-- a customized engine of the inner tags table must keep the kinds when it merges rows';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0
    TAGS ENGINE = ReplacingMergeTree ORDER BY (metric_name, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries TAGS INNER COLUMNS (sample_kinds UInt8); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries TAGS ENGINE = SummingMergeTree ORDER BY (metric_name, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_custom_engine ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0
    TAGS ENGINE = ReplacingMergeTree ORDER BY (metric_name, id, sample_kinds);
CREATE TABLE ts_custom_columns ENGINE = TimeSeries TAGS INNER COLUMNS (sample_kinds UInt8) TAGS ENGINE = MergeTree ORDER BY (metric_name, id);
CREATE TABLE ts_custom_memory ENGINE = TimeSeries TAGS INNER ENGINE = Memory;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 'ts\\_custom\\_%' ORDER BY name;

SELECT '-- the column must have one of the allowed types';
CREATE TABLE ts_bad_tags
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3),
    sample_kinds UInt16
) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ts_bad ENGINE = TimeSeries TAGS ts_bad_tags; -- { serverError BAD_TYPE_OF_FIELD }

DROP TABLE ts;
DROP TABLE ts_replacing;
DROP TABLE ts_no_time_ranges;
DROP TABLE ts_unfiltered;
DROP TABLE ts_external;
DROP TABLE ts_external_tags;
DROP TABLE ts_external_samples;
DROP TABLE ts_bad_tags;
DROP TABLE ts_custom_engine;
DROP TABLE ts_custom_columns;
DROP TABLE ts_custom_memory;
