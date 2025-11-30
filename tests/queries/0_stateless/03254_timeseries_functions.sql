CREATE TABLE ts_raw_data(timestamp DateTime64(3,'UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

INSERT INTO ts_raw_data SELECT arrayJoin(*).1::DateTime64(3, 'UTC') AS timestamp, arrayJoin(*).2 AS value
FROM (
select [
(1734955421.374, 0),
(1734955436.374, 0),
(1734955451.374, 0),
(1734955466.374, 0),
(1734955481.374, 0),
(1734955496.374, 0),
(1734955511.374, 1),
(1734955526.374, 3),
(1734955541.374, 5),
(1734955556.374, 5),
(1734955571.374, 5),
(1734955586.374, 5),
(1734955601.374, 8),
(1734955616.374, 8),
(1734955631.374, 8),
(1734955646.374, 8),
(1734955661.374, 8),
(1734955676.374, 8)
]);

SELECT groupArraySorted(20)((timestamp::Decimal(20,3), value)) FROM ts_raw_data;

SET allow_experimental_ts_to_grid_aggregate_function = 1;

WITH
    1734955380 AS start, 1734955680 AS end, 15 AS step, 0 AS staleness,
    timeSeriesRange(start, end, step) as grid
SELECT arrayZip(grid, timeSeriesResampleToGridWithStaleness(start, end, step, staleness)(timestamp, value)) FROM ts_raw_data;

WITH
    1734955380 AS start, 1734955680 AS end, 15 AS step, 300 AS window,
    timeSeriesRange(start, end, step) as grid
SELECT
    arrayZip(grid, timeSeriesLastToGrid(start, end, step, window)(timestamp, value)) as last_5m,
    arrayZip(grid, timeSeriesRateToGrid(start, end, step, window)(timestamp, value)) as rate_5m,
    arrayZip(grid, timeSeriesDeltaToGrid(start, end, step, window)(timestamp, value)) as delta_5m
FROM ts_raw_data FORMAT Vertical;

WITH
    1734955380 AS start, 1734955680 AS end, 15 AS step,
    timeSeriesRange(start, end, step) as grid
SELECT
    arrayZip(grid, timeSeriesInstantRateToGrid(start, end, step, 30)(timestamp, value)) as irate_30s, -- previous timestamp is within the window
    arrayZip(grid, timeSeriesInstantRateToGrid(start, end, step, 19)(timestamp, value)) as irate_19s, -- previous timestamp is still within the window
    arrayZip(grid, timeSeriesInstantRateToGrid(start, end, step, 18)(timestamp, value)) as irate_18s -- previous timestamp is outside the window
FROM ts_raw_data FORMAT Vertical;

DROP TABLE ts_raw_data;
