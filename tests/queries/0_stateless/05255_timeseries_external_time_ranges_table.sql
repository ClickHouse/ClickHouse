-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- A TimeSeries table can store the time ranges of its time series in an external table given by the TIME RANGES clause.
-- This test checks that inserts write such a table and that the selector filters time series by the ranges stored in it.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ext_time_ranges;

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

SELECT count() FROM timeSeriesSelector(ts_ext, 'm', 0, 2500);
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 0, 2500);
-- In [0, 1500] the only time series failing the matcher (h2) has no samples, so the probe decides that the selector matches the whole metric.
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 0, 1500);

SELECT '-- the filter is applied: a time series is hidden from a range which its stored time range does not intersect';
-- The stored time range of h1 is moved out of the ranges requested below. The selectors trust the stored time ranges,
-- so a selector filtering by them hides h1: this is the only way to see that it filters by them.
ALTER TABLE ext_time_ranges UPDATE min_time = toDateTime64(3000, 3), max_time = toDateTime64(3000, 3) WHERE min_time = toDateTime64(500, 3) SETTINGS mutations_sync = 1;
SELECT min_time, max_time FROM ext_time_ranges ORDER BY min_time;

-- A selector matching the whole metric doesn't filter time series by their stored time ranges, only the samples are filtered by their timestamps.
SELECT count() FROM timeSeriesSelector(ts_ext, 'm', 0, 2500);
-- h2 is in [0, 2500] and fails the matcher, so the time series are filtered by their stored time ranges and h1 is hidden (the correct result is 4).
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 0, 2500);
-- h2 has no samples in [0, 1500], so the probe decides that the selector matches the whole metric and the stored time range of h1 is not consulted.
SELECT count() FROM timeSeriesSelector(ts_ext, 'm{host="h1"}', 0, 1500);

DROP TABLE ts_ext;
DROP TABLE ext_time_ranges;
