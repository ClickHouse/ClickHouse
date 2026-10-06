SET allow_experimental_time_series_aggregate_functions = 1;

-- All queries use a single-point grid at timestamp 100 with the staleness window (90, 100].

SELECT 'The greatest value wins on duplicate timestamps:';

SELECT timeSeriesResampleToGridWithStaleness(100, 100, 1, 10)([95, 95]::Array(UInt32), [3, 5]::Array(Float64));
SELECT timeSeriesResampleToGridWithStaleness(100, 100, 1, 10)([95, 95]::Array(UInt32), [5, 3]::Array(Float64));

-- Two samples 6 seconds apart with values 6 apart give the slope exactly 1.
SELECT timeSeriesDerivToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 3, 7]::Array(Float64));
SELECT timeSeriesDerivToGrid(100, 100, 1, 10)([98, 98, 92]::Array(UInt32), [7, 3, 1]::Array(Float64));

SELECT timeSeriesInstantDeltaToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 3, 7]::Array(Float64));
SELECT timeSeriesInstantDeltaToGrid(100, 100, 1, 10)([98, 98, 92]::Array(UInt32), [7, 3, 1]::Array(Float64));

-- After deduplication the series is (92, 1), (98, 1): no changes.
SELECT timeSeriesChangesToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 1, 1]::Array(Float64));
-- After deduplication the series is (92, 1), (98, 5): one change.
SELECT timeSeriesChangesToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 1, 5]::Array(Float64));

SELECT 'NaN loses to a real value:';

SELECT timeSeriesDerivToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, nan, 7]::Array(Float64));
SELECT timeSeriesDerivToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 7, nan]::Array(Float64));

SELECT timeSeriesChangesToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, nan, 1]::Array(Float64));

SELECT timeSeriesInstantDeltaToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, nan, 7]::Array(Float64));
SELECT timeSeriesInstantDeltaToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 7, nan]::Array(Float64));

SELECT timeSeriesResampleToGridWithStaleness(100, 100, 1, 10)([95, 95]::Array(UInt32), [3, nan]::Array(Float64));
SELECT timeSeriesResampleToGridWithStaleness(100, 100, 1, 10)([95, 95]::Array(UInt32), [nan, 3]::Array(Float64));

SELECT 'NaN survives when all values at the timestamp are NaN:';

SELECT timeSeriesResampleToGridWithStaleness(100, 100, 1, 10)([95, 95]::Array(UInt32), [nan, nan]::Array(Float64));
SELECT timeSeriesInstantDeltaToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, nan, nan]::Array(Float64));
