-- Test exact rate, increase, and delta calculation without boundary extrapolation:
-- the optional fifth parameter `exact_rate` of timeSeriesRateToGrid, timeSeriesIncreaseToGrid and timeSeriesDeltaToGrid.
-- The PromQL side (setting `promql_exact_rate`) is tested in 05231_promql_exact_rate_translation.
SET enable_time_series_aggregate_functions = 1;

SELECT '--- Default extrapolation ---';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 0)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));

SELECT '--- Exact mode ---';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40, true)([100, 120]::Array(UInt32), [10, 20]::Array(Float64));

SELECT '--- Single sample returns NULL ---';
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1)([100]::Array(UInt32), [10]::Array(Float64));

SELECT '--- Counter reset ---';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 40, 1)([90, 100, 120]::Array(UInt32), [15, 5, 20]::Array(Float64));
SELECT timeSeriesIncreaseToGrid(120, 120, 1, 40, 1)([90, 100, 120]::Array(UInt32), [15, 5, 20]::Array(Float64));
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1)([90, 100, 120]::Array(UInt32), [15, 5, 20]::Array(Float64));

SELECT '--- Sliding window carries the previous sample ---';
SELECT timeSeriesIncreaseToGrid(100, 120, 10, 10, 1)([100, 110, 120]::Array(UInt32), [10, 20, 30]::Array(Float64));
SELECT timeSeriesDeltaToGrid(100, 120, 10, 10, 1)([100, 110, 120]::Array(UInt32), [10, 20, 30]::Array(Float64));

SELECT '--- Counter reset across window boundaries ---';
SELECT timeSeriesIncreaseToGrid(100, 120, 10, 10, 1)([100, 110, 120]::Array(UInt32), [100, 10, 25]::Array(Float64));

SELECT '--- DateTime64 timestamps ---';
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1)([100, 120]::Array(DateTime64(3)), [10, 20]::Array(Float64));

SELECT '--- The setting promql_exact_rate does not affect the aggregate functions used directly ---';
SELECT timeSeriesRateToGrid(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64)) SETTINGS promql_exact_rate = 1;
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64)) SETTINGS promql_exact_rate = 0;
SELECT toTypeName(timeSeriesRateToGridState(120, 120, 1, 40)([100, 120]::Array(UInt32), [10, 20]::Array(Float64))) SETTINGS promql_exact_rate = 1;
SELECT toTypeName(timeSeriesRateToGridState(120, 120, 1, 40, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64)));

SELECT '--- Invalid parameters ---';
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 'yes')([100, 120]::Array(UInt32), [10, 20]::Array(Float64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesRateToGrid(120, 120, 1, 40, 1, 1)([100, 120]::Array(UInt32), [10, 20]::Array(Float64)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '--- States keep the mode in their type: stored, detached, attached and merged ---';
DROP TABLE IF EXISTS t_promql_exact_rate_state;
CREATE TABLE t_promql_exact_rate_state
(
    id UInt8,
    exact_state AggregateFunction(timeSeriesRateToGrid(100, 120, 10, 20, 1), UInt32, Float64),
    default_state AggregateFunction(timeSeriesRateToGrid(100, 120, 10, 20), UInt32, Float64)
)
ENGINE = AggregatingMergeTree ORDER BY id;

-- Two parts, so OPTIMIZE has to merge the states.
INSERT INTO t_promql_exact_rate_state
SELECT 1, timeSeriesRateToGridState(100, 120, 10, 20, 1)(ts, val), timeSeriesRateToGridState(100, 120, 10, 20)(ts, val)
FROM values('ts UInt32, val Float64', (90, 0), (100, 10), (110, 20));
INSERT INTO t_promql_exact_rate_state
SELECT 1, timeSeriesRateToGridState(100, 120, 10, 20, 1)(ts, val), timeSeriesRateToGridState(100, 120, 10, 20)(ts, val)
FROM values('ts UInt32, val Float64', (115, 25), (120, 40));

SELECT finalizeAggregation(exact_state), finalizeAggregation(default_state) FROM t_promql_exact_rate_state ORDER BY _part;
SELECT timeSeriesRateToGridMerge(100, 120, 10, 20, 1)(exact_state), timeSeriesRateToGridMerge(100, 120, 10, 20)(default_state) FROM t_promql_exact_rate_state;

DETACH TABLE t_promql_exact_rate_state;
ATTACH TABLE t_promql_exact_rate_state;
OPTIMIZE TABLE t_promql_exact_rate_state FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_promql_exact_rate_state' AND active;
SELECT finalizeAggregation(exact_state), finalizeAggregation(default_state) FROM t_promql_exact_rate_state SETTINGS promql_exact_rate = 1;
SELECT finalizeAggregation(exact_state), finalizeAggregation(default_state) FROM t_promql_exact_rate_state SETTINGS promql_exact_rate = 0;

-- A state of one mode is not accepted by the function of the other one.
SELECT timeSeriesRateToGridMerge(100, 120, 10, 20)(exact_state) FROM t_promql_exact_rate_state; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_promql_exact_rate_state;
