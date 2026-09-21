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

SELECT '--- Sliding window carries the previous sample ---';
SELECT timeSeriesIncreaseToGrid(100, 120, 10, 10)([100, 110, 120]::Array(UInt32), [10, 20, 30]::Array(Float64));
SELECT timeSeriesDeltaToGrid(100, 120, 10, 10)([100, 110, 120]::Array(UInt32), [10, 20, 30]::Array(Float64));

SELECT '--- Counter reset across window boundaries ---';
SELECT timeSeriesIncreaseToGrid(100, 120, 10, 10)([100, 110, 120]::Array(UInt32), [100, 10, 25]::Array(Float64));

SELECT '--- Explicit parameter exact_rate = 1 regardless of setting ---';
SET promql_exact_rate = 0;
SELECT timeSeriesDeltaToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));

SELECT '--- Explicit parameter exact_rate = 0 regardless of setting ---';
SET promql_exact_rate = 1;
SELECT timeSeriesDeltaToGrid(120, 120, 1, 40, 0)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40, 0)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 0)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));

SELECT '--- Materialized state retains semantics under both settings ---';
DROP TABLE IF EXISTS t_promql_exact_rate_state;
CREATE TABLE t_promql_exact_rate_state ENGINE = Memory AS
SELECT
    timeSeriesIncreaseToGridState(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64)) AS inc_state,
    timeSeriesRateToGridState(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64)) AS rate_state;

SET promql_exact_rate = 0;
SELECT finalizeAggregation(inc_state) FROM t_promql_exact_rate_state;
SELECT finalizeAggregation(rate_state) FROM t_promql_exact_rate_state;
SELECT timeSeriesIncreaseToGridMerge(120, 120, 1, 40, 1)(inc_state) FROM t_promql_exact_rate_state;
SELECT timeSeriesRateToGridMerge(120, 120, 1, 40, 1)(rate_state) FROM t_promql_exact_rate_state;

SET promql_exact_rate = 1;
SELECT finalizeAggregation(inc_state) FROM t_promql_exact_rate_state;
SELECT finalizeAggregation(rate_state) FROM t_promql_exact_rate_state;
SELECT timeSeriesIncreaseToGridMerge(120, 120, 1, 40, 1)(inc_state) FROM t_promql_exact_rate_state;
SELECT timeSeriesRateToGridMerge(120, 120, 1, 40, 1)(rate_state) FROM t_promql_exact_rate_state;

DROP TABLE t_promql_exact_rate_state;

