-- Test exact rate, increase, and delta calculation without boundary extrapolation.
SET enable_time_series_aggregate_functions = 1;

SELECT '--- Default extrapolation (promql_exact_rate = 0) ---';
SET promql_exact_rate = 0;

SELECT timeSeriesDeltaToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));

SELECT '--- Exact rate mode (promql_exact_rate = 1) ---';
SET promql_exact_rate = 1;

SELECT timeSeriesDeltaToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));

SELECT '--- Single sample returns NULL ---';
SELECT timeSeriesRateToGrid(120, 120, 1, 40)([100]::Array(UInt32), [10]::Array(Float64));

SELECT '--- Counter reset with promql_exact_rate = 1 ---';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 40)([90, 100, 120]::Array(UInt32), [15, 5, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40)([90, 100, 120]::Array(UInt32), [15, 5, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40)([90, 100, 120]::Array(UInt32), [15, 5, 20]::Array(Float64));
