-- Regression test for issue #104459 / STID 4870 family:
-- `Logical error: AggregateFunctionArray: parameters mismatch between Array wrapper 'A'
--  and nested function 'B'`
--
-- The `timeSeries*ToGrid` aggregate function family normalizes its four timestamp parameters
-- internally to `Decimal64` (see `AggregateFunctionTimeseriesBase`). The `Array` combinator
-- used to store the user-supplied parameters (e.g. `UInt64_60`) while the nested function
-- stored the normalized parameters (e.g. `Decimal64_'60'`), so the wrapper's parameter
-- equality check raised `LOGICAL_ERROR` whenever `timeSeries*ToGridArray(...)` was invoked.
-- The fix has the `Array` combinator inherit the nested function's parameters — it is
-- semantically an adapter and its parameters should match the function it adapts.
-- See PR #104672 (revert of #103428) for why the alternative fix of dropping the
-- normalization is not viable: it breaks backward compatibility with already-serialized
-- timeseries state columns. This regression test pins the wrapper-level fix.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_104459;
-- `DateTime64(3, 'UTC')` keeps the timestamps stable across `session_timezone` randomization.
CREATE TABLE ts_104459 (timestamps Array(DateTime64(3, 'UTC')), values Array(Float64)) ENGINE=Memory;
INSERT INTO ts_104459 VALUES (
    [toDateTime64('1970-01-01 00:01:00', 3, 'UTC'),
     toDateTime64('1970-01-01 00:01:30', 3, 'UTC'),
     toDateTime64('1970-01-01 00:02:00', 3, 'UTC')],
    [10.0, 20.0, 30.0]);

-- Every 4-parameter variant of `timeSeries*ToGridArray`. Before the fix each of these
-- raised `LOGICAL_ERROR` because the wrapper saw `UInt64` params while the nested
-- function saw `Decimal64` params.
SELECT 'deltaArray',          timeSeriesDeltaToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'derivArray',          timeSeriesDerivToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'rateArray',           timeSeriesRateToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'instantDeltaArray',   timeSeriesInstantDeltaToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'instantRateArray',    timeSeriesInstantRateToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'changesArray',        timeSeriesChangesToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'resetsArray',         timeSeriesResetsToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'lastArray',           timeSeriesLastToGridArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'resampleArray',       timeSeriesResampleToGridWithStalenessArray(60, 180, 10, 20)(timestamps, values) FROM ts_104459;

-- 5-parameter `predict` variant. The nested function stores the fifth `predict_offset`
-- parameter as a member rather than in its parameter array, so the count differs too
-- (wrapper: 5 user-typed params, nested: 4 normalized params). The fix handles this
-- without special-casing.
SELECT 'predictLinearArray (5 params)',
       timeSeriesPredictLinearToGridArray(60, 180, 10, 20, -60)(timestamps, values) FROM ts_104459;

-- Decimal and String literals for the parameters: also normalized to Decimal64 by the
-- nested function and were previously rejected by the wrapper for the same reason.
SELECT 'decimalLiterals',
       timeSeriesDerivToGridArray(toDecimal64(60, 3), toDecimal64(180, 3), toDecimal64(10, 3), toDecimal64(20, 3))(timestamps, values)
FROM ts_104459;
SELECT 'stringLiterals',
       timeSeriesDerivToGridArray('60', '180', '10', '20')(timestamps, values) FROM ts_104459;

-- Stacked combinator (`ArrayIf`): both combinators must observe the same invariant.
SELECT 'arrayIfStacked',
       timeSeriesDerivToGridArrayIf(60, 180, 10, 20)(timestamps, values, 1) FROM ts_104459;

-- The non-`Array` variants must continue to work — sanity check that the wrapper-side fix
-- did not affect the timeseries functions themselves.
SELECT 'derivPlain',
       timeSeriesDerivToGrid(60, 180, 10, 20)(timestamps[1], values[1]) FROM ts_104459;
SELECT 'predictLinearPlain (5 params)',
       timeSeriesPredictLinearToGrid(60, 180, 10, 20, -60)(timestamps[1], values[1]) FROM ts_104459;

-- `INSERT-State` / `Merge` roundtrip through an `AggregateFunction` column: exercises the
-- column metadata encoding/decoding of the wrapper's parameters.
DROP TABLE IF EXISTS ts_state_104459;
CREATE TABLE ts_state_104459 (s AggregateFunction(timeSeriesLastToGridArray(60, 180, 10, 20), Array(DateTime64(3, 'UTC')), Array(Float64))) ENGINE=Memory;
INSERT INTO ts_state_104459 SELECT timeSeriesLastToGridArrayState(60, 180, 10, 20)(timestamps, values) FROM ts_104459;
SELECT 'mergedState',
       timeSeriesLastToGridArrayMerge(60, 180, 10, 20)(s) FROM ts_state_104459;

-- Non-timeseries aggregate functions wrapped with `Array`: the inherited parameters
-- equal the user-supplied ones (no normalization), so behaviour is unchanged.
SELECT 'sumArray',         sumArray([1, 2, 3, 4, 5]);
SELECT 'uniqArray',        uniqArray([1, 1, 2, 2, 3, 3, 4]);
SELECT 'quantilesArray',   quantilesArray(0.5, 0.9)([1, 2, 3, 4, 5, 100]);

-- Reproduction of the exact CI fuzzer query from issue #104459 (STID 4870-4f07).
WITH CAST('[(1600000000, 10), (1600000010, 20), (1600000020, 30)]', 'Array(Tuple(UInt32, Float64))') AS data
SELECT 'fuzzerReproducer',
       timeSeriesDeltaToGridArray(1600000010, 1600000050, 10, 30)(data.1, data.2);

DROP TABLE ts_state_104459;
DROP TABLE ts_104459;
