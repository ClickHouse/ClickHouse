-- The ts_of_* aggregates return a Unix timestamp in seconds, which must stay exact regardless of
-- the sample value column's type. A Float32 value column previously made the whole result round
-- to the nearest Float32, so a near-present-day epoch second like 1699999940 came back as
-- 1700000000, and 1699999880 came back as 1699999872.
SET allow_experimental_time_series_aggregate_functions = 1;

WITH
    [1699999880, 1699999940]::Array(DateTime) AS timestamps,
    [30, 10]::Array(Float32) AS values
SELECT
    timeSeriesTsOfFirstToGrid(1699999940, 1699999940, 1, 120)(timestamp, value),
    timeSeriesTsOfLastToGrid(1699999940, 1699999940, 1, 120)(timestamp, value),
    timeSeriesTsOfMinToGrid(1699999940, 1699999940, 1, 120)(timestamp, value),
    timeSeriesTsOfMaxToGrid(1699999940, 1699999940, 1, 120)(timestamp, value)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
