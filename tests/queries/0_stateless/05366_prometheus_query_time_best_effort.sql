-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

-- A time that is neither a number nor RFC3339 is read like parseDateTime64BestEffort reads it.
-- A number is still Unix seconds, and a datetime without a time zone is still read in the session time zone.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'Asia/Kolkata';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts (samples Array(Tuple(DateTime64(9), Float64))) ENGINE = TimeSeries;

SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('Tue, 14 Nov 2023 22:13:20 GMT', 9)) FROM prometheusQuery('ts', '1', 'Tue, 14 Nov 2023 22:13:20 GMT');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('Nov 14 2023 22:13:20 GMT', 9)) FROM prometheusQuery('ts', '1', 'Nov 14 2023 22:13:20 GMT');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('2023-11-14T22:13:20+0200', 9)) FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20+0200');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('2023-11-14T22:13:20+24:00', 9)) FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20+24:00');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('2023-11-14 22:13:20.123456789 +0530', 9)) FROM prometheusQuery('ts', '1', '2023-11-14 22:13:20.123456789 +0530');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('15 Nov 2023 03:43:20', 9)) FROM prometheusQuery('ts', '1', '15 Nov 2023 03:43:20');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('14 Nov 2023', 9)) FROM prometheusQuery('ts', '1', '14 Nov 2023');

-- Numbers keep meaning Unix seconds, even when they look like a date.
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '20231114');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '1700000000');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '-1');
SELECT toUnixTimestamp64Nano(timestamp) FROM prometheusQuery('ts', '1', '1700000000.5');

-- No number has two decimal points, so dotted dates are dates.
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('14.11.2023', 9)) FROM prometheusQuery('ts', '1', '14.11.2023');
SELECT toUnixTimestamp64Nano(timestamp) AS t, t = toUnixTimestamp64Nano(parseDateTime64BestEffort('2023.11.14', 9)) FROM prometheusQuery('ts', '1', '2023.11.14');
SELECT timeSeriesLastToGrid('14.11.2023', '14.11.2023', 1, 1)(parseDateTime64BestEffort('14.11.2023', 3), 1::Float64);
SELECT timeSeriesLastToGrid('2023.11.14', '2023.11.14', 1, 1)(parseDateTime64BestEffort('2023.11.14', 3), 1::Float64);

SELECT arrayMap(x -> toUnixTimestamp(x.1), samples) FROM prometheusQueryRange('ts', '1', 'Tue, 14 Nov 2023 22:13:20 GMT', '15 Nov 2023 03:44:20', '30s');
SELECT timeSeriesLastToGrid('Tue, 14 Nov 2023 22:13:20 GMT', '15 Nov 2023 03:44:20', 30, 60)(toDateTime(1700000000 + number * 30, 'UTC'), number::Float64) FROM numbers(3);

SELECT timestamp FROM prometheusQuery('ts', '1', 'now'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', 'abc'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '2023-11-14T22:13:20Z junk'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '1970-13-01'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '9223372036854775808'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '9223372036854775808.5'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '1.2.3'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', '1.2.3e5'); -- { serverError BAD_ARGUMENTS }

-- Like Prometheus, leading or trailing whitespace is an error, so a too-big number is never read as a date either.
SELECT timeSeriesLastToGrid(' 9223372036854775808', ' 9223372036854775816', 1, 1)(toDateTime64(0, 3, 'UTC'), 1.0); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid('\t9223372036854775808', '\t9223372036854775816', 1, 1)(toDateTime64(0, 3, 'UTC'), 1.0); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid('9223372036854775808 ', '9223372036854775816 ', 1, 1)(toDateTime64(0, 3, 'UTC'), 1.0); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid('- 9223372036854775809', '0', 1, 1)(toDateTime64(0, 3, 'UTC'), 1.0); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', ' 1700000000'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', ' 2023-11-14T22:13:20Z'); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts', '1', 'Tue, 14 Nov 2023 22:13:20 GMT '); -- { serverError BAD_ARGUMENTS }

DROP TABLE ts;
