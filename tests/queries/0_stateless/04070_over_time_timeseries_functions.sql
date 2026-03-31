-- Stateless test for Prometheus-style `*_over_time` aggregates mapped to a fixed grid:
-- `timeSeries*OverTimeToGrid(start, end, step, window)(timestamp, value)`.
--
-- Grid parameters below match a 120 second lookback (`window`, analogous to PromQL `[2m]`)
-- evaluated every 60 seconds (`step`) from `start` through `end` (inclusive end bucket).
--
-- Fixture data: 18 samples, 15 seconds apart; fractional part `.374` is arbitrary sub-second
-- noise. Aggregates use `toUnixTimestamp(timestamp)`

CREATE TABLE ts_raw_data(timestamp DateTime64(3,'UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

-- First sample unix second is start + 41 (start = 1734955380); spacing between rows is 15s.
INSERT INTO ts_raw_data SELECT arrayJoin(*).1::DateTime64(3, 'UTC') AS timestamp, arrayJoin(*).2 AS value
FROM (
select [
(1734955421.374, 0),
(1734955436.374, 0),
(1734955451.374, 1),
(1734955466.374, 1),
(1734955481.374, 1),
(1734955496.374, 3),
(1734955511.374, 3),
(1734955526.374, 3),
(1734955541.374, 5),
(1734955556.374, 3),
(1734955571.374, 3),
(1734955586.374, 3),
(1734955601.374, 2),
(1734955616.374, 4),
(1734955631.374, 6),
(1734955646.374, 8),
(1734955661.374, 8),
(1734955676.374, 8)
]);

SET allow_experimental_ts_to_grid_aggregate_function = 1;

-- Test avg_over_time, min_over_time, max_over_time, sum_over_time, count_over_time
WITH
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesAvgOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as avg_2m,
    arrayZip(grid, timeSeriesMinOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as min_2m,
    arrayZip(grid, timeSeriesMaxOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as max_2m,
    arrayZip(grid, timeSeriesSumOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as sum_2m,
    arrayZip(grid, timeSeriesCountOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as count_2m
FROM ts_raw_data FORMAT Vertical;

-- Test stddev_over_time, stdvar_over_time, present_over_time, absent_over_time
WITH
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesStddevOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stddev_2m,
    arrayZip(grid, timeSeriesStdvarOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stdvar_2m,
    arrayZip(grid, timeSeriesPresentOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as present_2m,
    arrayZip(grid, timeSeriesAbsentOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as absent_2m
FROM ts_raw_data FORMAT Vertical;

-- Test first_over_time, ts_of_last_over_time, ts_of_first_over_time
WITH
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesFirstOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as first_2m,
    arrayZip(grid, timeSeriesTsOfLastOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as ts_of_last_2m,
    arrayZip(grid, timeSeriesTsOfFirstOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as ts_of_first_2m
FROM ts_raw_data FORMAT Vertical;

-- Test ts_of_min_over_time, ts_of_max_over_time
WITH
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesTsOfMinOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as ts_of_min_2m,
    arrayZip(grid, timeSeriesTsOfMaxOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as ts_of_max_2m
FROM ts_raw_data FORMAT Vertical;

-- Test quantile_over_time (5 parameters: start, end, step, window, quantile)
WITH
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesQuantileOverTimeToGrid(start, end, step, window, 0.5)(toUnixTimestamp(timestamp), value)) as quantile50_2m,
    arrayZip(grid, timeSeriesQuantileOverTimeToGrid(start, end, step, window, 0.9)(toUnixTimestamp(timestamp), value)) as quantile90_2m
FROM ts_raw_data FORMAT Vertical;

-- Test mad_over_time
WITH
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesMadOverTimeToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as mad_2m
FROM ts_raw_data FORMAT Vertical;

-- Test with array arguments (same as above but passing arrays)
WITH
    [1734955421, 1734955436, 1734955451, 1734955466, 1734955481, 1734955496, 1734955511, 1734955526, 1734955541, 1734955556, 1734955571, 1734955586, 1734955601, 1734955616, 1734955631, 1734955646, 1734955661, 1734955676]::Array(DateTime) AS timestamps,
    [0, 0, 1, 1, 1, 3, 3, 3, 5, 3, 3, 3, 2, 4, 6, 8, 8, 8]::Array(Float64) AS values,
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesAvgOverTimeToGrid(start, end, step, window)(timestamps, values)) as avg_arr,
    arrayZip(grid, timeSeriesMinOverTimeToGrid(start, end, step, window)(timestamps, values)) as min_arr,
    arrayZip(grid, timeSeriesMaxOverTimeToGrid(start, end, step, window)(timestamps, values)) as max_arr
FORMAT Vertical;

DROP TABLE ts_raw_data;
