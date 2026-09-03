-- Tests the timeSeries{PredictLinear,Quantile}VaryingToGrid aggregate functions: the per-grid-point 3rd argument,
-- the value types it accepts, and the requirement that it is the same array in every row and in every merged state.
-- The PromQL functions built on them are tested end to end in 04551_promql_phase3_range_functions.

DROP TABLE IF EXISTS varying_input;

-- The functions are experimental and disabled by default.
SET allow_experimental_time_series_aggregate_functions = 0;
SET allow_experimental_time_series_table = 0;
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(toDateTime(100), 10::Float64, [0., 5., 10.]); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET allow_experimental_time_series_aggregate_functions = 1;

-- Samples (100, 10), (110, 20), (120, 30) on the grid [100, 110, 120] with a staleness window of 30 seconds.
-- `grp` splits the samples into two partial states for the merge tests below.
CREATE TABLE varying_input (grp UInt8, timestamp DateTime, value Float64, predict_offsets Array(Float64), phis Array(Float64)) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO varying_input VALUES (0, 100, 10, [0, 5, 10], [0, 0.5, 1]), (0, 110, 20, [0, 5, 10], [0, 0.5, 1]), (1, 120, 30, [0, 5, 10], [0, 0.5, 1]);

SELECT '-- timeSeries*VaryingToGrid: one parameter value per grid point';
-- The window of the first grid point holds a single sample, which is not enough for a linear fit.
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, predict_offsets) FROM varying_input;
SELECT timeSeriesQuantileVaryingToGrid(100, 120, 10, 30)(timestamp, value, phis) FROM varying_input;
-- The same with array arguments.
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamps, values, [0., 5., 10.]);
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesQuantileVaryingToGrid(100, 120, 10, 30)(timestamps, values, [0., 0.5, 1.]);

SELECT '-- timeSeries*VaryingToGrid: the 3rd argument accepts Float32 as well as Float64';
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, predict_offsets::Array(Float32)) FROM varying_input;
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value::Float32, predict_offsets::Array(Float32)) FROM varying_input;
SELECT timeSeriesQuantileVaryingToGrid(100, 120, 10, 30)(timestamp, value::Float32, phis::Array(Float32)) FROM varying_input;

SELECT '-- timeSeries*VaryingToGrid: partial states carrying the same arrays merge';
SELECT timeSeriesPredictLinearVaryingToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesPredictLinearVaryingToGridState(100, 120, 10, 30)(timestamp, value, predict_offsets) AS st FROM varying_input GROUP BY grp);
SELECT timeSeriesQuantileVaryingToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileVaryingToGridState(100, 120, 10, 30)(timestamp, value, phis) AS st FROM varying_input GROUP BY grp);

SELECT '-- timeSeries*VaryingToGrid: the 3rd argument must be the same in every row and in every merged state';
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x + grp, predict_offsets)) FROM varying_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileVaryingToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x / (1 + grp), phis)) FROM varying_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, arrayResize(predict_offsets, 3 - grp)) FROM varying_input; -- { serverError BAD_ARGUMENTS }
-- Rows that differ only in the middle of the array must be rejected as well.
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> if(x = 5, x + grp * 2, x), predict_offsets)) FROM varying_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileVaryingToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> if(x = 0.5, x + grp * 0.2, x), phis)) FROM varying_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesPredictLinearVaryingToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesPredictLinearVaryingToGridState(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x + grp, predict_offsets)) AS st FROM varying_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileVaryingToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileVaryingToGridState(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x / (1 + grp), phis)) AS st FROM varying_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }

SELECT '-- timeSeries*VaryingToGrid: invalid arguments';
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, [0., 5.]) FROM varying_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, [0, 5, 10]::Array(UInt64)) FROM varying_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesPredictLinearVaryingToGrid(100, 120, 10, 30)(timestamp, value, predict_offsets::Array(Nullable(Float64))) FROM varying_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileVaryingToGrid(100, 120, 10, 30)(timestamp, value) FROM varying_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE varying_input;
