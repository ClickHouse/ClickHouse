-- Regression test for STID 4701-4d85 (and siblings STID 3738-56e0, STID 4287-5905): adversarial
-- AST-fuzzer inputs with `window` near `INT64_MAX` triggered UBSan signed-integer overflow in the
-- `timeSeries*ToGrid` aggregate functions.
SET allow_experimental_time_series_aggregate_functions = 1;

-- Test cases 1 and 2 use `length` (the grid bucket count): the exact values are not the focus, only that
-- the call returns without aborting under UBSan. Test case 3 asserts actual values, because that overflow
-- dropped in-grid samples.

-- 1. isSampleOutOfWindow(): Condition `front().first + Base::window <= current_timestamp`
-- was able to cause overflow.
-- Here we reproduce the case with front().first = 1577862000 and Base::window = INT64_MAX,
-- because 1577862000 + INT64_MAX > INT64_MAX
WITH -9223372036854775808 AS start_time,                                 -- INT64_MIN
     9223372036854775807 AS end_time,                                    -- INT64_MAX
     1152921504606846976 AS step,                                        -- 2^60
     9223372036854775807 AS window,                                      -- INT64_MAX
     60 AS predict_offset,                                                -- seconds ahead, only used by timeSeriesPredictLinearToGrid
     [toDateTime64(1577862000, 0), toDateTime64(1577862500, 0)] AS timestamps,
     [1.0, 2.0] AS values
SELECT
    length(timeSeriesDerivToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesPredictLinearToGrid(start_time, end_time, step, window, predict_offset)(timestamps, values)),
    length(timeSeriesChangesToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesResetsToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesRateToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesDeltaToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesInstantRateToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesInstantDeltaToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesResampleToGridWithStaleness(start_time, end_time, step, window)(timestamps, values));

-- 2. fillResultValue(): Condition `current_timestamp - Base::window` was able to cause underflow.
-- Here we reproduce the case with current_timestamp = -2208988799 (1900-01-01 00:00:01)
-- and Base::window = INT64_MAX, because -2208988799 - INT64_MAX < INT64_MIN
WITH toDateTime64('1900-01-01 00:00:00', 0, 'UTC') AS start_time,
     toDateTime64('1900-01-01 00:00:10', 0, 'UTC') AS end_time,
     1 AS step,
     9223372036854775807 AS window,                                      -- INT64_MAX
     [toDateTime64('1900-01-01 00:00:00', 0, 'UTC'),                     -- bucket 0
      toDateTime64('1900-01-01 00:00:01', 0, 'UTC')] AS timestamps,      -- bucket 1
     [1.0, 2.0] AS values
SELECT
    length(timeSeriesRateToGrid(start_time, end_time, step, window)(timestamps, values)),
    length(timeSeriesDeltaToGrid(start_time, end_time, step, window)(timestamps, values));

-- 3. Base::add(): Condition `timestamp + window + step < start_time` was able to cause overflow
-- and drop in-grid samples.
-- Here we reproduce the case with start_time = 0, window = INT64_MAX, step = 1, timestamp = 1:
-- 1 + INT64_MAX + 1 overflowed and so it was considered negative (< start_time = 0),
-- so the sample was wrongly discarded.
WITH 0 AS start_time,
     10 AS end_time,
     1 AS step,
     9223372036854775807 AS window,                                      -- INT64_MAX
     [toDateTime64(1, 0)] AS timestamps,
     [5.0] AS values
SELECT timeSeriesResampleToGridWithStaleness(start_time, end_time, step, window)(timestamps, values);
