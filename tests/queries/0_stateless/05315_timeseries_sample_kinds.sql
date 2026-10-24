-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The time ranges table of a TimeSeries table with histograms keeps the kinds of samples of every time series in the column
-- `sample_kinds` (1 - float samples, 2 - histogram samples) next to its time range, and `timeSeriesSelector` reads only
-- the data tables of the kinds present. Without the time ranges table the kinds are unknown, and both tables are read.
-- Parallel replicas are disabled: the reads of the data tables must show up in the local plan. Every table gets a histogram
-- sample of another metric: the read of an empty table is planned as `ReadNothing`, not as `ReadFromMergeTree`.

SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 0;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_no_time_ranges;
DROP TABLE IF EXISTS ts_v9;
DROP TABLE IF EXISTS ts_bad;
DROP TABLE IF EXISTS ts_custom_key;
DROP TABLE IF EXISTS ts_custom_plain;

CREATE TABLE ts ENGINE = TimeSeries;

SELECT '-- the generated column is aggregated, and belongs to the time ranges table only';
SELECT countMatches(create_table_query, 'sample_kinds'), create_table_query LIKE '%`sample_kinds` SimpleAggregateFunction(groupBitOr, UInt8)%'
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

INSERT INTO ts (metric_name, tags, samples) VALUES ('floats', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('histograms', {'job': 'a'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
-- Both kinds in one row, and in two inserts.
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('mixed', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2)], [toDateTime64('2026-01-01 00:00:01', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('mixed', {'job': 'b'}, [(toDateTime64('2026-01-01 00:00:00', 3), 3)]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('mixed', {'job': 'b'}, [toDateTime64('2026-01-01 00:00:01', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
-- No samples: no row in the time ranges table.
INSERT INTO ts (metric_name, tags) VALUES ('empty', {'job': 'a'});

SELECT '-- the kinds stored in the time ranges table';
SELECT t.metric_name, t.tags['job'], groupBitOr(r.sample_kinds)
FROM timeSeriesTimeRanges(ts) AS r INNER JOIN timeSeriesTags(ts) AS t ON r.id = t.id GROUP BY ALL ORDER BY ALL;

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

SELECT '-- the rows of the two inserts of a series keep their kinds, and a merge ORs them into one row: kinds, then rows and kinds';
-- The rows may be merged already, so only the kinds are checked before the merge.
SELECT groupBitOr(sample_kinds) FROM timeSeriesTimeRanges(ts)
WHERE id IN (SELECT id FROM timeSeriesTags(ts) WHERE metric_name = 'mixed' AND tags['job'] = 'b');
OPTIMIZE TABLE ts FINAL;
SELECT count(), groupBitOr(sample_kinds) FROM timeSeriesTimeRanges(ts)
WHERE id IN (SELECT id FROM timeSeriesTags(ts) WHERE metric_name = 'mixed' AND tags['job'] = 'b');

SELECT '-- a series written with another id generator has an id outside the range of its metric: its kinds are unknown, both tables are read';
-- The series is written directly into the inner tables: the first component of its id isn't the hash of the metric name, so the
-- time ranges table isn't read for it (see `addTimeRangesTableJoin`) and the kinds stored there don't matter.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), 'floats', map('__name__', 'floats', 'job', 'old'));
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time, sample_kinds) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:00:00', 3), 1);
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) (id, timestamp, value) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), toDateTime64('2026-01-01 00:00:00', 3), 5);
SELECT 'floats', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'floats', value, length(histogram)
FROM timeSeriesSelector(ts, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)) ORDER BY ALL;

SELECT '-- without the time ranges table there is no column and no time filter: both tables are read, also for a series without samples';
CREATE TABLE ts_no_time_ranges ENGINE = TimeSeries SETTINGS store_time_ranges = 0;
SELECT countMatches(create_table_query, 'sample_kinds') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_no_time_ranges';
INSERT INTO ts_no_time_ranges (metric_name, tags, samples) VALUES ('floats', {}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts_no_time_ranges (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('other', {}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]]);
INSERT INTO ts_no_time_ranges (metric_name, tags) VALUES ('empty', {});
SELECT 'floats', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_no_time_ranges, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));
SELECT 'empty', countIf(explain LIKE '%ReadFromMergeTree%samples.%'), countIf(explain LIKE '%ReadFromMergeTree%histograms.%')
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(ts_no_time_ranges, 'empty', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3)));

SELECT '-- a table of a version without histograms has a time ranges table without the column, and the selector does not read it';
CREATE TABLE ts_v9 ENGINE = TimeSeries SETTINGS version = 9;
SELECT countMatches(create_table_query, 'sample_kinds') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v9';
INSERT INTO ts_v9 (metric_name, tags, samples) VALUES ('floats', {}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
SELECT 'floats', value FROM timeSeriesSelector(ts_v9, 'floats', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '-- a customized engine of the inner time ranges table must keep the kinds when it merges rows';
CREATE TABLE ts_bad ENGINE = TimeSeries TIME RANGES INNER ENGINE = ReplacingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries TIME RANGES INNER ENGINE = SummingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries TIME RANGES INNER COLUMNS (sample_kinds UInt8); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_custom_key ENGINE = TimeSeries TIME RANGES INNER COLUMNS (sample_kinds UInt8) TIME RANGES INNER ENGINE = ReplacingMergeTree ORDER BY (id, sample_kinds);
CREATE TABLE ts_custom_plain ENGINE = TimeSeries TIME RANGES INNER COLUMNS (sample_kinds UInt8) TIME RANGES INNER ENGINE = MergeTree ORDER BY id;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 'ts\\_custom\\_%' ORDER BY name;

SELECT '-- the column must have one of the allowed types';
CREATE TABLE ts_bad ENGINE = TimeSeries TIME RANGES INNER COLUMNS (sample_kinds UInt16); -- { serverError BAD_TYPE_OF_FIELD }

DROP TABLE ts;
DROP TABLE ts_no_time_ranges;
DROP TABLE ts_v9;
DROP TABLE ts_custom_key;
DROP TABLE ts_custom_plain;
