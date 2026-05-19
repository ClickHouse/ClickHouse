-- Tags: no-fasttest
-- Regression test for STID 4701-4d85 and sibling sightings (STID 3738-56e0, STID 4287-5905):
-- adversarial AST-fuzzer inputs that set the `window` (staleness) parameter to a value
-- near `INT64_MAX` triggered UBSan signed-integer overflow in the sliding-window check
-- `samples_in_window.front().first + Base::window <= current_timestamp` inside
-- `AggregateFunctionTimeseriesLinearRegression.h`, and the same pattern existed in
-- four sibling files (`AggregateFunctionTimeseriesChanges.h`,
-- `AggregateFunctionTimeseriesExtrapolatedValue.h`,
-- `AggregateFunctionTimeseriesInstantValue.h`,
-- `AggregateFunctionTimeseriesToGridSparse.h`). The query below exercises every affected
-- `timeSeries*ToGrid` function with `window = 9223372036854775807` (i.e. `INT64_MAX`) and a
-- positive sample timestamp; before the fix this aborted the server under UBSan.
SET allow_experimental_time_series_aggregate_functions = 1;

-- Each query must complete without aborting the server. The exact NULL/0 pattern is
-- not the focus of this test (single-sample inputs cannot produce a non-NULL rate or
-- linear-regression value); what matters is that the call returns instead of crashing.
SELECT length(timeSeriesDerivToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0]::Array(Float64)));

SELECT length(timeSeriesPredictLinearToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64,
    60)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0]::Array(Float64)));

SELECT length(timeSeriesChangesToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0]::Array(Float64)));

SELECT length(timeSeriesResetsToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0]::Array(Float64)));

SELECT length(timeSeriesRateToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)'), reinterpret(1577862500::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0, 2.0]::Array(Float64)));

SELECT length(timeSeriesDeltaToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)'), reinterpret(1577862500::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0, 2.0]::Array(Float64)));

SELECT length(timeSeriesInstantRateToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)'), reinterpret(1577862500::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0, 2.0]::Array(Float64)));

SELECT length(timeSeriesInstantDeltaToGrid(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)'), reinterpret(1577862500::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0, 2.0]::Array(Float64)));

SELECT length(timeSeriesResampleToGridWithStaleness(
    -9223372036854775808::Int64,
    9223372036854775807::Int64,
    1152921504606846976::Int64,
    9223372036854775807::Int64)(
        [reinterpret(1577862000::Int64, 'DateTime64(0)')]::Array(DateTime64(0)),
        [1.0]::Array(Float64)));
