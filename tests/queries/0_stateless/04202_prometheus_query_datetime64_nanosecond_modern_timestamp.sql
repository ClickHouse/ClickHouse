-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it.

-- Regression test for `timeSeriesTimestampToAST`. With `DateTime64(9)` and a
-- modern Unix timestamp the raw value (e.g. 1.7e18 for 2024-01-01) exceeds
-- the precision of `Decimal64` (18 digits), so the timestamp literal must be
-- wrapped via `toDecimal128`, not `toDecimal64`, before conversion to
-- `DateTime64`. Otherwise `prometheusQuery` throws `DECIMAL_OVERFLOW`.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_ns;

-- The samples are stored with nanosecond timestamps.
CREATE TABLE ts_ns (samples Array(Tuple(DateTime64(9, 'UTC'), Float64))) ENGINE = TimeSeries;

-- A modern Unix timestamp (2024-01-01) on a `DateTime64(9)` table.
-- Without the `toDecimal128` wrap this would throw `DECIMAL_OVERFLOW`.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '1 + 2', 1704067200);

-- Far-future timestamp, well past `Decimal64` precision.
SELECT timestamp, value FROM prometheusQuery('ts_ns', '10 * 5', 4102444800);

-- Small timestamp still works (the path that needs decimal wrapping for the
-- year-vs-seconds ambiguity).
SELECT timestamp, value FROM prometheusQuery('ts_ns', '7', 1000);

DROP TABLE ts_ns;
