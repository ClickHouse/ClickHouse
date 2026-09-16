-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: table functions like `timeSeriesTimeRanges` read the inner tables directly and don't work
-- with parallel replicas, and the test asserts on the query plan shape.
--
-- A TimeSeries table stores the time range of each time series in the "time ranges" target table,
-- tables of version 7 or earlier store it in the columns `min_time` and `max_time` of the "tags" table (see TimeSeriesVersion.h).
-- The generation of the definition is covered by the unit test gtest_normalize_time_series_definition.cpp,
-- this test checks the write path and the filtering of time series by their time ranges.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET log_queries = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_no_ranges;
DROP TABLE IF EXISTS ts_v7;
DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ext_time_ranges;

CREATE TABLE ts ENGINE = TimeSeries;

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
SELECT min_time, max_time FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:String}, 'ts') ORDER BY min_time;

SELECT '-- the selector reads the time ranges table to filter time series by time';
SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 2500);
SYSTEM FLUSH LOGS query_log;
SELECT arrayExists(t -> t LIKE '%.inner\_id.timeranges.%', tables) AS reads_time_ranges FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM timeSeriesSelector(ts, ''m{host="h1"}'', 0, 2500)%'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- the probe deciding whether a selector matches the whole metric reads the time ranges table too: a time series without samples in the range is not a counterexample';
-- h2 fails the matcher: in [0, 2500] it is a counterexample, so the id range is not emitted and the stored time ranges filter the ids;
-- in [0, 1600] it is ignored, so the selector counts as matching the whole metric and the ids are not filtered.
SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN indexes = 1 SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 2500)));
SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN indexes = 1 SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 1600)));

SELECT '-- store_time_ranges = 0 disables the time ranges table and the filtering';
CREATE TABLE ts_no_ranges ENGINE = TimeSeries SETTINGS store_time_ranges = 0;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.timeranges.%';
INSERT INTO ts_no_ranges (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);
INSERT INTO ts_no_ranges (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);
SELECT count() FROM timeSeriesSelector(ts_no_ranges, 'm{host="h1"}', 0, 2500);
SYSTEM FLUSH LOGS query_log;
SELECT arrayExists(t -> t LIKE '%.inner\_id.timeranges.%', tables) AS reads_time_ranges FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM timeSeriesSelector(ts\_no\_ranges, ''m{host="h1"}'', 0, 2500)%'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- a TIME RANGES clause requires store_time_ranges to be enabled and is rejected for version 7 or earlier';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_time_ranges = 0 TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 7 TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- a table of version 7 keeps min_time and max_time in the tags table and filters by them';
CREATE TABLE ts_v7 ENGINE = TimeSeries SETTINGS version = 7;
INSERT INTO ts_v7 (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);
INSERT INTO ts_v7 (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);
OPTIMIZE TABLE ts_v7 FINAL;
SELECT min_time, max_time FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts_v7') ORDER BY min_time;
SELECT count() FROM timeSeriesSelector(ts_v7, 'm{host="h1"}', 0, 2500);
SELECT count() FROM timeSeriesSelector(ts_v7, 'm{host="h1"}', 2000, 3000);

SELECT '-- an external time ranges table is written by inserts and read by the selector';
CREATE TABLE ext_time_ranges
(
    id Tuple(UInt64, LowCardinality(UUID)),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY id;
CREATE TABLE ts_ext ENGINE = TimeSeries TIME RANGES ext_time_ranges;
INSERT INTO ts_ext (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('m', map('host', 'h3'), []);
INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(500, 3), 0.), (toDateTime64(1500, 3), 4.)]);
OPTIMIZE TABLE ext_time_ranges FINAL;
SELECT min_time, max_time FROM ext_time_ranges ORDER BY min_time;
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 0, 2500);

SELECT '-- the filter is applied: a time series is hidden from a range which its stored time range does not intersect';
ALTER TABLE ext_time_ranges UPDATE max_time = toDateTime64(1000, 3) WHERE min_time = toDateTime64(500, 3) SETTINGS mutations_sync = 1;
-- h1 has a sample at 1500, so the correct result is 1. The result is 0 because the stored time range of h1 is broken above:
-- the selector trusts the stored time ranges, and this is the only way to see that it filters by them.
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 1200, 2500);
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 0, 2500);

SELECT '-- a selector matching the whole metric does not filter time series by their stored time ranges, only the samples are filtered by their timestamps';
SELECT count() FROM timeSeriesSelector(ts_ext, 'm', 1200, 2500);
-- h2 has no samples in [1200, 1600], so the probe ignores it and the matcher selector counts as matching the whole metric too.
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 1200, 1600);

SELECT '-- DROP TABLE drops the inner tables';
DROP TABLE ts_ext;
DROP TABLE ext_time_ranges;
DROP TABLE ts_v7;
DROP TABLE ts_no_ranges;
DROP TABLE ts;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';
