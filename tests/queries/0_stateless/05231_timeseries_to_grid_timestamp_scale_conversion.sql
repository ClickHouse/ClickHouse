-- The grid parameters of the timeSeries*ToGrid functions can have a scale different from the scale of the timestamps of the samples:
-- the grid takes the greater of the two scales (but at least milliseconds) and the timestamps are converted to it. So UInt32,
-- DateTime and DateTime64(1) timestamps must give the same results as the same samples stored as DateTime64(3). When no timestamp
-- of the column can fall into the buckets of the grid, the grid has no buckets and rejects every sample.

SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_scales;

CREATE TABLE ts_scales (ts_u32 UInt32, ts_dt DateTime('UTC'), ts_dt64_1 DateTime64(1, 'UTC'), ts_dt64_3 DateTime64(3, 'UTC'), ts_dt64_6 DateTime64(6, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY ts_u32;

-- The samples at 0 and 4294967295 (the range of UInt32) are outside the windows of the grid [1000.5, 1010.5] used by most queries.
INSERT INTO ts_scales SELECT ts, toDateTime(ts, 'UTC'), toDateTime64(ts, 1, 'UTC'), toDateTime64(ts, 3, 'UTC'), toDateTime64(ts, 6, 'UTC'), value
FROM VALUES('ts UInt32, value Float64', (0, 10), (1000, 1), (1001, 2), (1002, 4), (1005, 3), (1006, 16), (1010, 32), (4294967295, 20));

-- The grid: start 1000.5, end 1010.5, step 2.5 s, window 3.5 s, all with millisecond precision.
-- `ts_dt` is compared only once: DateTime and UInt32 timestamps are the same UInt32 values inside the functions.
SELECT 'rate';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt, value) = expected,
    timeSeriesRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'increase';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesIncreaseToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesIncreaseToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesIncreaseToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'irate';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesInstantRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesInstantRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesInstantRateToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'deriv';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesDerivToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesDerivToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesDerivToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'predict_linear';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesPredictLinearToGrid(grid_start, grid_end, grid_step, grid_window, 10)(ts_dt64_3, value) AS expected,
    timeSeriesPredictLinearToGrid(grid_start, grid_end, grid_step, grid_window, 10)(ts_u32, value) = expected,
    timeSeriesPredictLinearToGrid(grid_start, grid_end, grid_step, grid_window, 10)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'changes';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesChangesToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesChangesToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesChangesToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'resets';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesResetsToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesResetsToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesResetsToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'last';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected,
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected
FROM ts_scales;

SELECT 'sum, avg, count';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesSumToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_sum,
    timeSeriesSumToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected_sum,
    timeSeriesAvgToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_avg,
    timeSeriesAvgToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected_avg,
    timeSeriesCountToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_count,
    timeSeriesCountToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected_count
FROM ts_scales;

SELECT 'max, min, ts_of_max, ts_of_min';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesMaxToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_max,
    timeSeriesMaxToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) = expected_max,
    timeSeriesMinToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_min,
    timeSeriesMinToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) = expected_min,
    timeSeriesTimestampOfMaxToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_ts_of_max,
    timeSeriesTimestampOfMinToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected_ts_of_min
FROM ts_scales;

-- The timestamps of the maximums and minimums keep the type of the timestamps, so they are compared as seconds.
SELECT 'ts_of_max, ts_of_min with other timestamp types';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesTimestampOfMaxToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) AS ts_of_max_u32,
    CAST(ts_of_max_u32, 'Array(Nullable(Float64))') = CAST(timeSeriesTimestampOfMaxToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value), 'Array(Nullable(Float64))'),
    timeSeriesTimestampOfMinToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt, value) AS ts_of_min_dt,
    CAST(ts_of_min_dt, 'Array(Nullable(Float64))') = CAST(timeSeriesTimestampOfMinToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value), 'Array(Nullable(Float64))'),
    timeSeriesTimestampOfMinToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_1, value) AS ts_of_min_dt64_1
FROM ts_scales;

-- Samples passed as arrays are converted too.
SELECT 'arrays';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(rows.ts_dt64_3, rows.value) AS expected,
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(rows.ts_u32, rows.value) = expected,
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(rows.sample_dt) = expected
FROM (SELECT groupArray((ts_dt64_3, ts_u32, value, (ts_dt, value))::Tuple(ts_dt64_3 DateTime64(3, 'UTC'), ts_u32 UInt32, value Float64, sample_dt Tuple(DateTime('UTC'), Float64))) AS rows FROM ts_scales);

-- A grid with integer parameters still works with UInt32 timestamps.
SELECT 'integer parameters';
SELECT timeSeriesLastToGrid(1000, 1010, 5, 3)(ts_u32, value), timeSeriesRateToGrid(1000, 1010, 5, 3)(ts_dt, value) FROM ts_scales;

-- Fractional Float and String parameters are not truncated to whole seconds: the grid has at least millisecond precision.
SELECT 'fractional Float and String parameters';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(ts_u32, value) AS expected,
    timeSeriesLastToGrid(1000.5, 1010.5, 2.5, 3.5)(ts_u32, value) = expected,
    timeSeriesLastToGrid('1000.5', '1010.5', '2.5', '3.5')(ts_dt, value) = expected,
    timeSeriesLastToGrid('1970-01-01 00:16:40.5', '1970-01-01 00:16:50.5', '2500ms', '3500ms')(ts_dt64_1, value) = expected
FROM ts_scales;

-- Parameters with a smaller scale than the timestamps are widened to the scale of the timestamps, which becomes the scale of the grid.
SELECT 'parameters with a smaller scale than the timestamps';
WITH toDateTime64(1000.5, 3, 'UTC') AS grid_start, toDateTime64(1010.5, 3, 'UTC') AS grid_end, toDecimal64(2.5, 3) AS grid_step, toDecimal64(3.5, 3) AS grid_window
SELECT
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_3, value) AS expected,
    timeSeriesLastToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_6, value) = expected,
    timeSeriesLastToGrid(1000.5, 1010.5, 2.5, 3.5)(ts_dt64_6, value) = expected,
    timeSeriesTimestampOfMaxToGrid(grid_start, grid_end, grid_step, grid_window)(ts_dt64_6, value) AS ts_of_max_dt64_6
FROM ts_scales;

-- The buckets of the grid are converted to the scale of the column. When no timestamp of the column falls into them
-- (they lie outside the range of the type, or between two consecutive timestamps), every sample is rejected.
SELECT 'grid before the epoch';
SELECT timeSeriesCountToGrid(-100, -60, 20, 30)(ts_dt, value), timeSeriesMaxToGrid(-100, -60, 20, 30)(ts_dt, value) FROM ts_scales;

SELECT 'grid ending at the epoch: only the last window reaches the sample at 0';
SELECT timeSeriesCountToGrid(-40, 0, 20, 30)(ts_dt, value), timeSeriesMaxToGrid(-40, 0, 20, 30)(ts_dt, value) FROM ts_scales;

SELECT 'grid above the range of DateTime';
SELECT timeSeriesCountToGrid(4294967400, 4294967440, 20, 30)(ts_dt, value), timeSeriesMaxToGrid(4294967400, 4294967440, 20, 30)(ts_u32, value) FROM ts_scales;

SELECT 'grid starting at the maximum of DateTime: the windows of the first two grid points reach the sample at 4294967295';
SELECT timeSeriesCountToGrid(4294967295, 4294967335, 20, 30)(ts_dt, value), timeSeriesMaxToGrid(4294967295, 4294967335, 20, 30)(ts_u32, value) FROM ts_scales;

SELECT 'buckets between two consecutive seconds';
SELECT timeSeriesCountToGrid(1000.2, 1000.8, 0.3, 0.1)(ts_dt, value), timeSeriesCountToGrid(1000.5, 1000.5, 1, 0.2)(ts_dt, value) FROM ts_scales;

SELECT 'a wider window reaches the sample at 1000';
SELECT timeSeriesCountToGrid(1000.2, 1000.8, 0.3, 0.3)(ts_dt, value), timeSeriesCountToGrid(1000.5, 1000.5, 1, 1)(ts_dt, value) FROM ts_scales;

SELECT 'buckets between two consecutive milliseconds of DateTime64(3)';
SELECT
    timeSeriesCountToGrid(toDateTime64(1000.0002, 6, 'UTC'), toDateTime64(1000.0008, 6, 'UTC'), toDecimal64(0.0003, 6), toDecimal64(0.0001, 6))(ts_dt64_3, value),
    timeSeriesCountToGrid(toDateTime64(1000.0002, 6, 'UTC'), toDateTime64(1000.0008, 6, 'UTC'), toDecimal64(0.0003, 6), toDecimal64(0.0003, 6))(ts_dt64_3, value)
FROM ts_scales;

DROP TABLE ts_scales;
