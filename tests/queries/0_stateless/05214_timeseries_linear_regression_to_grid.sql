-- Tests timeSeriesLinearRegressionToGrid: for every grid point it returns the tuple (intercept, slope) of the line fitted to
-- the samples in the window, where `intercept` is the value of the line at the grid point's timestamp and `slope` is per
-- second. The PromQL function `predict_linear` is built on it, see 05215_promql_predict_linear.

SET enable_time_series_aggregate_functions = 1;

-- Samples (100, 10), (110, 20), (120, 30) on the grid [100, 110, 120] with a staleness window of 30 seconds.
-- The window of the first grid point holds a single sample, which is not enough for a fit.
SELECT '-- result type and values';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT toTypeName(timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamps, values)) SETTINGS print_pretty_type_names = 0;
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamps, values);

SELECT '-- the same samples passed as rows and as an array of tuples';
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamp, value)
FROM (SELECT arrayJoin([(100, 10.), (110, 20.), (120, 30.)]) AS sample, toDateTime(sample.1) AS timestamp, sample.2 AS value);
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(arrayZip(timestamps, values));

SELECT '-- the slope is per second regardless of the timestamp precision, and the result follows the value type';
WITH [100, 110, 120]::Array(DateTime64(3)) AS timestamps, [10, 20, 30]::Array(Float32) AS values
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamps, values) AS regression, toTypeName(regression) SETTINGS print_pretty_type_names = 0;

SELECT '-- intercept + slope * offset is timeSeriesPredictLinearToGrid, slope is timeSeriesDerivToGrid';
WITH
    [100, 110, 120]::Array(DateTime) AS timestamps,
    [10, 20, 30]::Array(Float64) AS values,
    timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamps, values) AS regression
SELECT
    arrayMap(r -> r.intercept + r.slope * 60, regression),
    timeSeriesPredictLinearToGrid(100, 120, 10, 30, 60)(timestamps, values),
    arrayMap(r -> r.slope, regression),
    timeSeriesDerivToGrid(100, 120, 10, 30)(timestamps, values);

SELECT '-- a different offset for every grid point';
WITH
    [100, 110, 120]::Array(DateTime) AS timestamps,
    [10, 20, 30]::Array(Float64) AS values,
    timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamps, values) AS regression
SELECT arrayMap((r, t) -> r.intercept + r.slope * t, regression, [0., 5., 10.]);

SELECT '-- partial states merge';
SELECT timeSeriesLinearRegressionToGridMerge(100, 120, 10, 30)(st)
FROM (SELECT timeSeriesLinearRegressionToGridState(100, 120, 10, 30)(timestamp, value) AS st
      FROM (SELECT arrayJoin([(100, 10., 0), (110, 20., 0), (120, 30., 1)]) AS sample, toDateTime(sample.1) AS timestamp, sample.2 AS value, sample.3 AS grp)
      GROUP BY grp);

SELECT '-- a grid without samples';
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamp, value) FROM (SELECT toDateTime(0) AS timestamp, 0. AS value WHERE 0);

SELECT '-- invalid arguments';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30, 60)(timestamps, values); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesLinearRegressionToGrid(100, 120, 10, 30)([1, 2, 3]::Array(UInt32), 1.); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
