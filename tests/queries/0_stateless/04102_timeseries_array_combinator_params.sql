-- Regression test for STID 4870 family:
-- "AggregateFunctionArray: parameters mismatch between Array wrapper ... and nested function ..."
--
-- TimeSeries aggregate functions used to normalize their parameters to Decimal64
-- internally (and silently drop `predict_offset` from the 5-param `timeSeriesPredictLinearToGrid`).
-- The `Array` combinator wraps the nested function and asserts
-- `parameters == nested_func->getParameters()`, so any time the caller used non-Decimal64
-- literals (or the 5-param predict variant) the assertion fired as `LOGICAL_ERROR`.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_data_4102;
-- Use DateTime64(3, 'UTC') so that session_timezone randomization does not shift the
-- timestamps out of the grid range — this test only verifies absence of LOGICAL_ERROR,
-- but we also want the numeric output to be stable across sessions.
CREATE TABLE ts_data_4102 (timestamps Array(DateTime64(3, 'UTC')), values Array(Float64)) ENGINE=Memory;
INSERT INTO ts_data_4102 VALUES (
    [toDateTime64('1970-01-01 00:01:00', 3, 'UTC'),
     toDateTime64('1970-01-01 00:02:00', 3, 'UTC'),
     toDateTime64('1970-01-01 00:03:00', 3, 'UTC')],
    [10.0, 20.0, 30.0]);

-- 4-parameter variants with UInt64 literals:
-- Before the fix these threw LOGICAL_ERROR: wrapper had [UInt64_60, ...] while the nested
-- function had [Decimal64_'60', ...].
SELECT 'derivArray' AS name, timeSeriesDerivToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_data_4102;
SELECT 'resetsArray' AS name, timeSeriesResetsToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_data_4102;
SELECT 'changesArray' AS name, timeSeriesChangesToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_data_4102;
SELECT 'rateArray' AS name, timeSeriesRateToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_data_4102;
SELECT 'instantRateArray' AS name, timeSeriesInstantRateToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_data_4102;
SELECT 'resampleArray' AS name, timeSeriesResampleToGridWithStalenessArray(60, 180, 10, 20)(timestamps, values) FROM ts_data_4102;

-- 5-parameter `predict` variant. Before the fix, the wrapper stored 5 params
-- ([UInt64_60, UInt64_180, UInt64_10, UInt64_20, Int64_-60]) and the nested function
-- stored only 4 ([Decimal64_'60', Decimal64_'180', Decimal64_'10', Decimal64_'20']);
-- the fifth `predict_offset` was dropped.
SELECT 'predictLinearArray (5 params)' AS name,
       timeSeriesPredictLinearToGridArray(60, 180, 10, 20, -60)(timestamps, values) FROM ts_data_4102;

-- Mixed parameter types and combinator stacking.
SELECT 'decimalParamsArray' AS name,
       timeSeriesDerivToGridArray(toDecimal64(60, 3), toDecimal64(180, 3), toDecimal64(10, 3), toDecimal64(20, 3))(timestamps, values)
FROM ts_data_4102;

SELECT 'stringParamsArray' AS name,
       timeSeriesDerivToGridArray('60', '180', '10', '20')(timestamps, values) FROM ts_data_4102;

SELECT 'stackedArrayIf' AS name,
       timeSeriesDerivToGridArrayIf(60, 180, 10, 20)(timestamps, values, 1) FROM ts_data_4102;

-- Non-Array variants must still work (regression check).
SELECT 'deriv (no Array)' AS name,
       timeSeriesDerivToGrid(60, 180, 10, 20)(timestamps[1], values[1]) FROM ts_data_4102;
SELECT 'predictLinear (no Array, 5 params)' AS name,
       timeSeriesPredictLinearToGrid(60, 180, 10, 20, -60)(timestamps[1], values[1]) FROM ts_data_4102;

DROP TABLE ts_data_4102;
