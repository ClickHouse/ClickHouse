CREATE TABLE ts_raw_data(timestamp DateTime64(3,'UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

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

SELECT groupArraySorted(20)((timestamp::Decimal(20,3), value)) FROM ts_raw_data;

SET allow_experimental_ts_to_grid_aggregate_function = 1;

WITH
    1734955380 AS start, 1734955680 AS end, 15 AS step, 300 AS window, 60 as predict_offset,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesChangesToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as changes_5m,
    arrayZip(grid, timeSeriesResetsToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as resets_5m
FROM ts_raw_data FORMAT Vertical;

DROP TABLE ts_raw_data;
