-- Tests timeSeriesPresentToGrid: for every grid point it returns 1 if the window has at least one sample and NULL
-- otherwise. The PromQL functions built on it are tested in 05211_promql_present_over_time_absent_over_time.

-- The function is in private preview and disabled by default.
SET enable_time_series_aggregate_functions = 0;
SET enable_time_series_table = 0;
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)(toDateTime(100), 10::Float64); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

-- Samples (100, 10), (110, 20), (120, 30) on the grid [100, 110, 120] with a staleness window of 30 seconds.
SELECT '-- result type and values';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)(timestamps, values) AS present, toTypeName(present);

SELECT '-- a grid point whose window has no samples is NULL';
-- Grid [100, 140, 180] with a 30 second window: the windows (70, 100] and (110, 140] have samples, (150, 180] has none.
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesPresentToGrid(100, 180, 40, 30)(timestamps, values);

SELECT '-- the values do not matter: NaN samples are present too';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [nan, nan, nan]::Array(Float64) AS values
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)(timestamps, values);

SELECT '-- the same samples passed as rows and as an array of tuples';
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)(timestamp, value)
FROM (SELECT arrayJoin([(100, 10.), (110, 20.), (120, 30.)]) AS sample, toDateTime(sample.1) AS timestamp, sample.2 AS value);
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)(arrayZip(timestamps, values));

SELECT '-- partial states merge';
-- Grid [100, 120, 140, 160] with a 30 second window: the last window (130, 160] has no samples.
SELECT timeSeriesPresentToGridMerge(100, 160, 20, 30)(st)
FROM (SELECT timeSeriesPresentToGridState(100, 160, 20, 30)(timestamp, value) AS st
      FROM (SELECT arrayJoin([(100, 10., 0), (120, 30., 1)]) AS sample, toDateTime(sample.1) AS timestamp, sample.2 AS value, sample.3 AS grp)
      GROUP BY grp);

SELECT '-- a grid without samples';
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)(timestamp, value) FROM (SELECT toDateTime(0) AS timestamp, 0. AS value WHERE 0);

SELECT '-- invalid arguments';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesPresentToGrid(100, 120, 10, 30, 60)(timestamps, values); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesPresentToGrid(100, 120, 10, 30)([1, 2, 3]::Array(UInt32), 1.); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
