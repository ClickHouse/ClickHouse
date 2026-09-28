-- Tests timeSeriesQuantileToGrid: the `phi` argument (one quantile level for the whole grid or one level per grid point,
-- the value types it accepts, and the requirement that it is the same in every row and in every merged state) and the
-- treatment of NaN samples. The PromQL function built on it is tested in 05213_promql_quantile_over_time.

DROP TABLE IF EXISTS quantile_input;

-- The function is in private preview and disabled by default.
SET enable_time_series_aggregate_functions = 0;
SET enable_time_series_table = 0;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(toDateTime(100), 10::Float64, 0.5); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

-- Samples (100, 10), (110, 20), (120, 30) on the grid [100, 110, 120] with a staleness window of 30 seconds, so the
-- windows hold {10}, {10, 20} and {10, 20, 30}. `grp` splits the samples into two partial states for the merge tests below.
CREATE TABLE quantile_input (grp UInt8, timestamp DateTime, value Float64, phis Array(Float64)) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO quantile_input VALUES (0, 100, 10, [0, 0.5, 1]), (0, 110, 20, [0, 0.5, 1]), (1, 120, 30, [0, 0.5, 1]);

SELECT '-- one quantile level for the whole grid';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 0.5) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 1) FROM quantile_input;
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(arrayZip(timestamps, values), 0.5);

SELECT '-- one quantile level per grid point';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, phis) FROM quantile_input;
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamps, values, [0., 0.5, 1.]);
-- An array with the same level at every grid point is the same as that level.
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [0.5, 0.5, 0.5]) FROM quantile_input;

SELECT '-- the level accepts any numbers: Float32, Float64 and integers';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value::Float32, phis::Array(Float32)) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 1::UInt8) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [0, 1, 1]::Array(UInt64)) FROM quantile_input;

SELECT '-- a level outside [0, 1] gives -Inf or +Inf and a NaN level gives NaN, like in Prometheus';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, -0.5) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 1.5) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, nan) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [-1., 0.5, 2.]) FROM quantile_input;
-- A grid point without samples stays NULL whatever the level: the window (150, 180] of the last grid point is empty.
SELECT timeSeriesQuantileToGrid(100, 180, 40, 30)(timestamp, value, 2) FROM quantile_input;

SELECT '-- rows excluded by a condition or by a NULL are ignored, whatever their level';
SELECT timeSeriesQuantileToGridIf(100, 120, 10, 30)(timestamp, value, 0.5 + grp, grp = 0) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, if(grp = 0, value, NULL), 0.5 + grp) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, if(grp = 0, 0.5, NULL)) FROM quantile_input;
-- Any non-zero condition includes the row.
SELECT timeSeriesQuantileToGridIf(100, 120, 10, 30)(timestamp, value, 0.5, toUInt8(2)) FROM quantile_input;

SELECT '-- partial states carrying the same level merge';
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, phis) AS st FROM quantile_input GROUP BY grp);
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, 0.5) AS st FROM quantile_input GROUP BY grp);

SELECT '-- the level must be the same in every row and in every merged state';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 0.5 + grp) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x / (1 + grp), phis)) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
-- Rows that differ only in the middle of the array must be rejected as well.
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> if(x = 0.5, x + grp * 0.2, x), phis)) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, 0.5 + grp) AS st FROM quantile_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x / (1 + grp), phis)) AS st FROM quantile_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }

SELECT '-- invalid arguments';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [0., 0.5]) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, phis::Array(Nullable(Float64))) FROM quantile_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, '0.5') FROM quantile_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value) FROM quantile_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30, 0.5)(timestamp, value) FROM quantile_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE quantile_input;

-- NaN samples follow Prometheus, whose `vectorByValueHeap.Less` sorts NaN before every real value instead of dropping
-- it, so a window of [1, NaN, 2] sorts as [NaN, 1, 2] and its median is 1 rather than the 1.5 of a NaN-free [1, 2].

SELECT '-- a NaN sample is kept and sorts before every real value';
-- Grid [100, 110] with a 30 second window: the first grid point sees only (100, 1),
-- the second one sees all three samples.
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, 0.5);
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, [0.5, 0.5]);

SELECT '-- phi = 0 selects the NaN that sorts first, phi = 1 selects the largest real value';
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, 0);
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, 1);
-- The same with one level per grid point: phi = 0.5 at the first grid point, phi = 0 at the second one.
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, [0.5, 0]);

SELECT '-- a window with only NaN samples yields NaN, a window without samples stays NULL';
-- Grid [100, 120, 140] with a 30 second window: the last grid point has no samples at all.
WITH [100, 110]::Array(DateTime) AS timestamps, [nan, nan]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 140, 20, 30)(timestamps, values, 0.5);
WITH [100, 110]::Array(DateTime) AS timestamps, [nan, nan]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 140, 20, 30)(timestamps, values, [0.5, 0.5, 0.5]);
