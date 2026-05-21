-- Regression test for the `Float64` precision loss in `duration_to_end` (and `duration_to_start`)
-- inside `fillResultValue` in `AggregateFunctionTimeseriesExtrapolatedValue.h`. The previous
-- form `static_cast<Float64>(current_timestamp) - static_cast<Float64>(last_timestamp)` rounded
-- each operand to ~256 ns quanta at present-day `DateTime64(9)` epoch (~1.7e18) before the
-- subtraction, so exact 1 ns deltas were silently zeroed. The subtraction now happens in
-- `Int128` first and converts to `Float64` once.

SET allow_experimental_time_series_aggregate_functions = 1;

-- Grid point at 1700000000000000001 ns, two samples at 1699999999999999998 ns and
-- 1700000000000000000 ns. The latest sample is exactly 1 ns before the grid point.
--
-- `Int64(1700000000000000001) - Int64(1700000000000000000) = 1` (exact).
-- `Float64(1700000000000000001) - Float64(1700000000000000000) = 0` (`~256` ns quantum at
-- this magnitude, both round to the same representable `Float64`).
--
-- With the precision bug, `duration_to_end = 0`, so the extrapolated interval is
-- `sampled_interval (2) + duration_to_start_clamped (1.0) + 0 = 3` and the result is `1.5`.
-- After the fix, `duration_to_end = 1.0`, the interval is `4`, and the result is `2.0`.

SELECT timeSeriesRateToGrid(
    reinterpret(1700000000000000001::Int64, 'DateTime64(9, ''UTC'')'),
    reinterpret(1700000000000000001::Int64, 'DateTime64(9, ''UTC'')'),
    1::Int64,
    1000000000::Int64
)(
    [reinterpret(1699999999999999998::Int64, 'DateTime64(9, ''UTC'')'),
     reinterpret(1700000000000000000::Int64, 'DateTime64(9, ''UTC'')')]::Array(DateTime64(9, 'UTC')),
    [1.0, 2.0]::Array(Float64)) AS rate;

SELECT timeSeriesDeltaToGrid(
    reinterpret(1700000000000000001::Int64, 'DateTime64(9, ''UTC'')'),
    reinterpret(1700000000000000001::Int64, 'DateTime64(9, ''UTC'')'),
    1::Int64,
    1000000000::Int64
)(
    [reinterpret(1699999999999999998::Int64, 'DateTime64(9, ''UTC'')'),
     reinterpret(1700000000000000000::Int64, 'DateTime64(9, ''UTC'')')]::Array(DateTime64(9, 'UTC')),
    [1.0, 2.0]::Array(Float64)) AS delta_;
