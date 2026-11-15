-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

-- A time in RFC3339 format with 'Z' or an offset is read in its own time zone.
-- A datetime without a time zone is still read in the session time zone.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'Asia/Kolkata';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts (samples Array(Tuple(DateTime64(9), Float64))) ENGINE = TimeSeries;

SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '1700000000.123456789');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20.123456789Z');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '2023-11-15T00:13:20.123456789+02:00');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '2023-11-14T17:13:20.123456789-05:00');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20Z');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '2023-11-15 03:43:20');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '2023-11-15T03:43:20');
SELECT arrayMap(x -> toUnixTimestamp(x.1), samples) FROM prometheusQueryRange('ts', '1', '2023-11-14T22:13:20Z', '2023-11-15T00:14:20+02:00', '30s');
SELECT timeSeriesLastToGrid('2023-11-14T22:13:20Z', '2023-11-15T00:14:20+02:00', 30, 60)(toDateTime(1700000000 + number * 30, 'UTC'), number::Float64) FROM numbers(3);

SELECT timestamp FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20+24:00'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20+0200'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20z'); -- { serverError BAD_ARGUMENTS }

DROP TABLE ts;
