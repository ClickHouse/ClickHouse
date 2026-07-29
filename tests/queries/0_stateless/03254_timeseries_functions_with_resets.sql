-- Counter resets: `rate` and `increase` count a reset (a decrease between
-- consecutive samples is treated as a counter restart), while `delta` treats the
-- input as a gauge and ignores resets. `increase` equals `rate` multiplied by the
-- window, and `timeSeriesResetsToGrid` reports how many resets fall into the window.

CREATE TABLE ts_raw_data(timestamp DateTime, value Float64) ENGINE = MergeTree() ORDER BY timestamp;

-- Counter rising to 10, resetting to 2 at ts = 160, rising to 9, resetting to 1 at ts = 205.
INSERT INTO ts_raw_data VALUES
(100, 1), (115, 3), (130, 6), (145, 10), (160, 2), (175, 5), (190, 9), (205, 1), (220, 4);

SET allow_experimental_ts_to_grid_aggregate_function = 1;

WITH
    90::UInt32 AS start, 225::UInt32 AS end, 15 AS step, 60 AS window,
    timeSeriesRange(start, end, step) AS grid
SELECT
    arrayZip(grid, timeSeriesRateToGrid(start, end, step, window)(timestamp, value)) AS rate,
    arrayZip(grid, timeSeriesIncreaseToGrid(start, end, step, window)(timestamp, value)) AS increase,
    arrayZip(grid, timeSeriesDeltaToGrid(start, end, step, window)(timestamp, value)) AS delta,
    arrayZip(grid, timeSeriesResetsToGrid(start, end, step, window)(timestamp, value)) AS resets
FROM ts_raw_data FORMAT Vertical;

DROP TABLE ts_raw_data;
