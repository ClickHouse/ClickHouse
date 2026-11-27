SET allow_experimental_time_series_aggregate_functions = 1;

SELECT 'The greatest value wins on duplicate timestamps:';

SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [3, 5]::Array(Float64));
SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [5, 3]::Array(Float64));

SELECT 'NaN loses to a real value:';

SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [3, nan]::Array(Float64));
SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [nan, 3]::Array(Float64));
SELECT timeSeriesGroupArray([90, 95, 95, 98]::Array(UInt32), [1, nan, 5, 2]::Array(Float64));

SELECT 'NaN survives when all values at the timestamp are NaN:';

SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [nan, nan]::Array(Float64));
SELECT timeSeriesGroupArray([90, 95, 95]::Array(UInt32), [1, nan, nan]::Array(Float64));
