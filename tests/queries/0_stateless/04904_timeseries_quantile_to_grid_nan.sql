-- Tests how the timeSeriesQuantile*ToGrid aggregate functions treat NaN samples: NaN is skipped before the values
-- are sorted (the same invariant as QuantileExactBase::add), and a window holding only NaN samples still yields NaN.

SET allow_experimental_time_series_aggregate_functions = 1;

SELECT '-- a NaN sample is skipped: the median of [1, NaN, 2] is the median of [1, 2]';
-- Grid [100, 110] with a 30 second window: the first grid point sees only (100, 1),
-- the second one sees all three samples.
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30, 0.5)(timestamps, values);
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileVaryingToGrid(100, 110, 10, 30)(timestamps, values, [0.5, 0.5]);

SELECT '-- a window with only NaN samples yields NaN, a window without samples stays NULL';
-- Grid [100, 120, 140] with a 30 second window: the last grid point has no samples at all.
WITH [100, 110]::Array(DateTime) AS timestamps, [nan, nan]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 140, 20, 30, 0.5)(timestamps, values);
WITH [100, 110]::Array(DateTime) AS timestamps, [nan, nan]::Array(Float64) AS values
SELECT timeSeriesQuantileVaryingToGrid(100, 140, 20, 30)(timestamps, values, [0.5, 0.5, 0.5]);
