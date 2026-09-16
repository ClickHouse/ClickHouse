-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: plain `DETACH TABLE` is not allowed there, only `DETACH TABLE PERMANENTLY`.
--
-- From version 7 a TimeSeries table stores the time range of each time series in the "time ranges" target table
-- instead of the columns `min_time` and `max_time` of the "tags" table (see TimeSeriesVersion.h).
-- The generation of the definition is covered by the unit test gtest_normalize_time_series_definition.cpp,
-- this test checks the created inner tables, the write path and the filtering of time series by their time ranges.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_no_ranges;
DROP TABLE IF EXISTS ts_v6;
DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ext_time_ranges;

CREATE TABLE ts ENGINE = TimeSeries;

SELECT '-- the inner tags table is ReplacingMergeTree, the inner time ranges table is AggregatingMergeTree ordered by id';
-- An inner table is named `.inner_id.<target>.<uuid>`, so `splitByChar('.', name)[3]` is the target kind.
SELECT splitByChar('.', name)[3] AS target, engine, sorting_key, primary_key FROM system.tables
WHERE database = currentDatabase() AND (name LIKE '.inner\_id.tags.%' OR name LIKE '.inner\_id.timeranges.%') ORDER BY target;

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.timeranges.%' ORDER BY position;

SELECT '-- the tags table has no min_time and max_time columns';
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.tags.%' AND name IN ('min_time', 'max_time');

SELECT '-- an insert writes the time range of each inserted time series with samples';
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);

-- The database is passed to the table functions explicitly, otherwise the queries fail with parallel replicas:
-- the functions read the inner MergeTree table directly, so the query can be sent to another replica where the current database is different.
SELECT min_time, max_time FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:String}, 'ts') ORDER BY min_time;

SELECT '-- a second insert extends the time range, the ranges of a time series are merged into one';
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);
OPTIMIZE TABLE ts FINAL;
SELECT count() FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:String}, 'ts');
SELECT min_time, max_time FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:String}, 'ts') ORDER BY min_time;

SELECT '-- the selector reads the time ranges table to filter time series by time';
SELECT count() FROM timeSeriesSelector(ts, 'm', 0, 1600);
SELECT plan LIKE '%.inner_id.timeranges.%' AS reads_time_ranges
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT count() FROM timeSeriesSelector(ts, 'm', 0, 1600)));

SELECT '-- the table survives DETACH/ATTACH: the write path keeps working';
DETACH TABLE ts;
ATTACH TABLE ts;
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h2'), [(toDateTime64(3000, 3), 5.)]);
SELECT max(max_time) FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:String}, 'ts');

SELECT '-- store_time_ranges = 0 disables the time ranges table and the filtering';
CREATE TABLE ts_no_ranges ENGINE = TimeSeries SETTINGS store_time_ranges = 0;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.timeranges.%';
INSERT INTO ts_no_ranges (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.)]);
SELECT value FROM timeSeriesSelector(ts_no_ranges, 'm', 0, 2000);
SELECT plan LIKE '%timeranges%' AS reads_time_ranges
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT count() FROM timeSeriesSelector(ts_no_ranges, 'm', 0, 2000)));

SELECT '-- a TIME RANGES clause requires store_time_ranges to be enabled';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_time_ranges = 0 TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- the settings of the columns min_time and max_time of the tags table apply to tables of versions before 7';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS filter_by_min_time_and_max_time = 0; -- { serverError INVALID_SETTING_VALUE }
ALTER TABLE ts MODIFY SETTING filter_by_min_time_and_max_time = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 6, store_time_ranges = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 6 TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- a table of version 6 keeps min_time and max_time in the tags table and has no time ranges table';
CREATE TABLE ts_v6 ENGINE = TimeSeries SETTINGS version = 6;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.timeranges.%';
SELECT name FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.tags.%' AND name IN ('min_time', 'max_time') ORDER BY name;
INSERT INTO ts_v6 (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.)]);
SELECT min_time, max_time FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_v6');
SELECT value FROM timeSeriesSelector(ts_v6, 'm', 0, 2000);
SELECT count() FROM timeSeriesSelector(ts_v6, 'm', 2000, 3000);
ALTER TABLE ts_v6 MODIFY SETTING filter_by_min_time_and_max_time = 0;
SELECT value FROM timeSeriesSelector(ts_v6, 'm', 0, 2000);

SELECT '-- an external time ranges table is written by inserts and read by the selector';
CREATE TABLE ext_time_ranges
(
    id Tuple(UInt64, LowCardinality(UUID)),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY id;
CREATE TABLE ts_ext ENGINE = TimeSeries TIME RANGES ext_time_ranges;
INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.)]);
SELECT min_time, max_time FROM ext_time_ranges;
SELECT value FROM timeSeriesSelector(ts_ext, 'm', 0, 2000);

SELECT '-- the filter is applied: a time range which does not intersect the requested one hides the time series';
TRUNCATE TABLE ext_time_ranges;
INSERT INTO ext_time_ranges SELECT id, toDateTime64(5000, 3), toDateTime64(6000, 3) FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_ext');
SELECT count() FROM timeSeriesSelector(ts_ext, 'm', 0, 2000);

SELECT '-- DROP TABLE drops the inner tables';
DROP TABLE ts_ext;
DROP TABLE ext_time_ranges;
DROP TABLE ts_v6;
DROP TABLE ts_no_ranges;
DROP TABLE ts;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';
