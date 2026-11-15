-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it.

-- Regression test: a non-finite Float64 (NaN/inf) argument used as a
-- timestamp/duration in prometheusQuery / prometheusQueryRange must produce a
-- clean error instead of undefined behaviour. Previously a NaN bypassed the
-- overflow range check in getFromFloat (every comparison with NaN is false),
-- so static_cast<Int64>(NaN) executed - undefined behaviour caught by UBSan.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_ns;

-- The samples are stored with nanosecond timestamps.
CREATE TABLE ts_ns (samples Array(Tuple(DateTime64(9, 'UTC'), Float64))) ENGINE = TimeSeries;

-- Timestamp path: getFromFloat<DateTime64>.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', -nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', inf); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', -inf); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', 0. / 0.); -- { serverError BAD_ARGUMENTS }

SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', toFloat64(9223372036.854776)); -- { serverError BAD_ARGUMENTS }

-- Duration path: getFromFloat<Decimal64> (the step argument of prometheusQueryRange).
SELECT timestamp, value FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, 2000, nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, 2000, inf); -- { serverError BAD_ARGUMENTS }
SELECT timestamp, value FROM prometheusQueryRange('ts_ns', '1 + 2', 1000, 2000, toFloat64(9223372036.854776)); -- { serverError BAD_ARGUMENTS }

-- A finite float timestamp still works (sanity: the guard does not reject valid input).
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', 1704067200.0);

DROP TABLE ts_ns;
