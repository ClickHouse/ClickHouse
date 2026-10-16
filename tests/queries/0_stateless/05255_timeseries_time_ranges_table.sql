-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: table functions like `timeSeriesTimeRanges` read the inner tables directly and don't work
-- with parallel replicas.
--
-- A TimeSeries table stores the time range of each time series in the "time ranges" target table,
-- tables of version 7 or earlier store it in the columns `min_time` and `max_time` of the "tags" table (see TimeSeriesVersion.h).
-- The generation of the definition is covered by the unit test gtest_normalize_time_series_definition.cpp,
-- this test checks the write path and which tables the selector reads; the filtering of time series by their time ranges
-- is checked in 05255_timeseries_external_time_ranges_table.sql.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET log_queries = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_no_ranges;
DROP TABLE IF EXISTS ts_v7;

SELECT '-- a table has an inner time ranges table';
CREATE TABLE ts ENGINE = TimeSeries;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.timeranges.%';

SELECT '-- an insert writes the time range of each inserted time series with samples';
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);
SELECT min_time, max_time FROM timeSeriesTimeRanges(ts) ORDER BY min_time;

SELECT '-- a second insert extends the time range';
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);

SELECT '-- a selector returns the samples of the matching time series in the time range, the ranges of a time series are not merged yet';
SELECT count() FROM timeSeriesSelector(ts, 'm', 0, 2500) SETTINGS log_comment = 'ts_m';
SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 2500) SETTINGS log_comment = 'ts_m_h1';
-- In [0, 1500] the only time series failing the matcher (h2) has no samples, so the probe decides that the selector matches the whole metric.
SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 1500) SETTINGS log_comment = 'ts_m_h1_1500';

SELECT '-- the ranges of a time series are merged into one';
OPTIMIZE TABLE ts FINAL;
SELECT min_time, max_time FROM timeSeriesTimeRanges(ts) ORDER BY min_time;

SELECT '-- store_time_ranges = 0 disables the time ranges table';
CREATE TABLE ts_no_ranges ENGINE = TimeSeries SETTINGS store_time_ranges = 0;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.timeranges.%';
INSERT INTO ts_no_ranges (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);
INSERT INTO ts_no_ranges (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);
SELECT count() FROM timeSeriesSelector(ts_no_ranges, 'm', 0, 2500) SETTINGS log_comment = 'ts_no_ranges_m';
SELECT count() FROM timeSeriesSelector(ts_no_ranges, 'm{host="h1"}', 0, 2500) SETTINGS log_comment = 'ts_no_ranges_m_h1';
-- Without stored time ranges the probe has no time information, so h2 stays a counterexample in [0, 1500] and the selector is not treated as matching the whole metric.
SELECT count() FROM timeSeriesSelector(ts_no_ranges, 'm{host="h1"}', 0, 1500) SETTINGS log_comment = 'ts_no_ranges_m_h1_1500';

SELECT '-- a TIME RANGES clause requires store_time_ranges to be enabled and is rejected for version 7 or earlier';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_time_ranges = 0 TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 7 TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- a table of version 7 keeps min_time and max_time in the tags table';
CREATE TABLE ts_v7 ENGINE = TimeSeries SETTINGS version = 7;
INSERT INTO ts_v7 (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);
INSERT INTO ts_v7 (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);
OPTIMIZE TABLE ts_v7 FINAL;
SELECT min_time, max_time FROM timeSeriesTags(ts_v7) ORDER BY min_time;
SELECT count() FROM timeSeriesSelector(ts_v7, 'm', 0, 2500) SETTINGS log_comment = 'ts_v7_m';
SELECT count() FROM timeSeriesSelector(ts_v7, 'm{host="h1"}', 0, 2500) SETTINGS log_comment = 'ts_v7_m_h1';
SELECT count() FROM timeSeriesSelector(ts_v7, 'm{host="h1"}', 0, 1500) SETTINGS log_comment = 'ts_v7_m_h1_1500';

SELECT '-- the selector reads the time ranges table only if the table has one';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, arrayExists(t -> t LIKE '%.inner\_id.timeranges.%', tables) AS reads_time_ranges FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('ts_m', 'ts_m_h1', 'ts_m_h1_1500', 'ts_no_ranges_m', 'ts_no_ranges_m_h1', 'ts_no_ranges_m_h1_1500', 'ts_v7_m', 'ts_v7_m_h1', 'ts_v7_m_h1_1500')
ORDER BY log_comment;

DROP TABLE ts_v7;
DROP TABLE ts_no_ranges;
DROP TABLE ts;
