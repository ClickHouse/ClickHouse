-- Grid parameters of the timeSeries*ToGrid functions near the limits of Int64 (found by the AST fuzzer and UBSan):
-- parameters which don't fit the grid are rejected with BAD_ARGUMENTS, and the arithmetic with values close to the limits
-- doesn't overflow. The grid has at least millisecond precision, so the closest an integer parameter (seconds) can get
-- to the limits of Int64 is +-9223372036854775.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_extreme;
CREATE TABLE ts_extreme (timestamp DateTime64(0, 'UTC'), timestamp_ms DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ts_extreme VALUES ('2020-01-01 00:00:00', '2020-01-01 00:00:00', 1.0), ('2020-01-01 00:00:01', '2020-01-01 00:00:01', 2.0);

SELECT '-- parameters which do not fit the grid';
SELECT timeSeriesLastToGrid(-9223372036854775808, 256, 2147483646, 2147483648)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid(0, 9223372036854775807, 1, 1)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid(0, 10, 9223372036854775807, 1)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid(0, 10, 1, 9223372036854775807)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
-- UInt64 values above the maximum of Int64 must not wrap to negative timestamps.
SELECT timeSeriesLastToGrid(9223372036854775808, 150, 15, 50)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid(18446744073709551615, 150, 15, 50)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }

SELECT '-- end before start';
SELECT timeSeriesChangesToGrid(9223372036854775, 1, 256, 2147483648)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }

SELECT '-- too many grid points: the limit is 16777215';
SELECT timeSeriesLastToGrid(-9223372036854775, 256, 2147483, 2147483)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid(-9223372036854775, 9223372036854775, 1, 1)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesLastToGrid(0, 16777215, 1, 1)(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT length(timeSeriesLastToGrid(0, 16777214, 1, 1)(timestamp, value)) FROM ts_extreme;
SELECT length(timeSeriesChangesToGrid(0, 16777213, 1, 1)(toDateTime(timestamp), value)) FROM ts_extreme;

SELECT '-- a grid of three points spanning the whole range: no overflow while calculating the grid points';
WITH -9223372036854775 AS grid_start, 9223372036854775 AS grid_end, 9223372036854775 AS grid_step
SELECT
    length(timeSeriesLastToGrid(grid_start, grid_end, grid_step, 0)(timestamp, value)),
    length(timeSeriesInstantRateToGrid(grid_start, grid_end, grid_step, 0)(timestamp, value)),
    length(timeSeriesChangesToGrid(grid_start, grid_end, grid_step, 0)(timestamp, value)),
    length(timeSeriesRateToGrid(grid_start, grid_end, grid_step, 0)(timestamp, value)),
    length(timeSeriesDerivToGrid(grid_start, grid_end, grid_step, 0)(timestamp, value))
FROM ts_extreme;

SELECT '-- the samples fall into the last bucket of that grid: no overflow while calculating the bucket index';
SELECT arrayFirstIndex(x -> x IS NOT NULL, timeSeriesLastToGrid(-9223372036854775, 9223372036854775, 9223372036854775, 9223372036854775)(timestamp, value))
FROM ts_extreme;

SELECT '-- the same grid on a DateTime64(3) column: the timestamps are not converted to the scale of the grid';
WITH -9223372036854775 AS grid_start, 9223372036854775 AS grid_end, 9223372036854775 AS grid_step
SELECT
    length(timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_step)(timestamp_ms, value)),
    arrayFirstIndex(x -> x IS NOT NULL, timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_step)(timestamp_ms, value))
FROM ts_extreme;

SELECT '-- the exact limits of Int64 as the bounds of the grid: only String parameters keep the milliseconds exactly';
SELECT
    length(timeSeriesLastToGrid('-9223372036854775.808', '9223372036854775.807', 9223372036854775, 9223372036854775)(timestamp, value)),
    arrayFirstIndex(x -> x IS NOT NULL, timeSeriesLastToGrid('-9223372036854775.808', '9223372036854775.807', 9223372036854775, 9223372036854775)(timestamp, value))
FROM ts_extreme;

SELECT '-- a window near the maximum: no overflow while comparing the samples with the windows';
WITH -9223372036854775 AS grid_start, 9223372036854775 AS grid_end, 1152921504606846 AS grid_step, 9223372036854775 AS grid_window
SELECT
    length(timeSeriesDerivToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesPredictLinearToGrid(grid_start, grid_end, grid_step, grid_window, 60)(timestamp, value)),
    length(timeSeriesChangesToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesResetsToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesRateToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesDeltaToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesInstantRateToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesInstantDeltaToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value)),
    length(timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(timestamp, value))
FROM ts_extreme;

-- The start of a window before 1970 minus a window near the maximum is below the minimum of Int64.
WITH toDateTime64('1900-01-01 00:00:00', 0, 'UTC') AS grid_start, toDateTime64('1900-01-01 00:00:10', 0, 'UTC') AS grid_end, 9223372036854775 AS grid_window,
     [toDateTime64('1900-01-01 00:00:00', 0, 'UTC'), toDateTime64('1900-01-01 00:00:01', 0, 'UTC')] AS timestamps, [1.0, 2.0] AS values
SELECT length(timeSeriesRateToGrid(grid_start, grid_end, 1, grid_window)(timestamps, values)), length(timeSeriesDeltaToGrid(grid_start, grid_end, 1, grid_window)(timestamps, values));

-- The sample at 1 is in the window of every grid point after it: `timestamp + window` is above the maximum of Int64.
SELECT timeSeriesLastToGrid(0, 10, 1, 9223372036854775)([toDateTime64(1, 0, 'UTC')], [5.0]);

SELECT '-- Decimal parameters are converted to the scale of the grid, which is the greatest scale among the parameters';
SELECT length(timeSeriesChangesToGrid(toDecimal32(1000.5, 3), toDecimal32(1010.5, 3), toDecimal64(2.5, 3), toDecimal64(3.5, 9))(timestamp, value)) FROM ts_extreme;
SELECT timeSeriesChangesToGrid(toDecimal32(1000.5, 3), toDecimal32(1010.5, 3), toDecimal64(2.5, 3), toDecimal64(3.5, 17))(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesChangesToGrid(0, toDecimal64(9223372036854775, 3), 1, toDecimal64(1, 4))(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesChangesToGrid(toDecimal64(-9223372036854775, 3), 0, 1, toDecimal64(1, 4))(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesChangesToGrid(0, 10, toDecimal64(9223372036854775, 3), toDecimal64(1, 4))(timestamp, value) FROM ts_extreme; -- { serverError BAD_ARGUMENTS }

DROP TABLE ts_extreme;
