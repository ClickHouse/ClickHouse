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

SET allow_experimental_time_series_aggregate_functions = 1;

WITH
    1734955380 AS start, 1734955680 AS end, 15 AS step, 300 AS window,
    timeSeriesRange(start, end, step) as grid
SELECT
    arrayZip(grid, timeSeriesSumToGrid(start, end, step, window)(timestamp, value)) as sum_5m,
    arrayZip(grid, timeSeriesAvgToGrid(start, end, step, window)(timestamp, value)) as avg_5m,
    arrayZip(grid, timeSeriesCountToGrid(start, end, step, window)(timestamp, value)) as count_5m
FROM ts_raw_data FORMAT Vertical;

-- Grid points 150, 165 and 180 have no samples in their window and must be NULL, not 0.
WITH
    90::UInt32 AS start, 230::UInt32 AS end, 15 AS step, 10 AS window,
    timeSeriesRange(start, end, step) as grid
SELECT
    arrayZip(grid, timeSeriesSumToGrid(start, end, step, window)(timestamp, value)) as sum_gap,
    arrayZip(grid, timeSeriesAvgToGrid(start, end, step, window)(timestamp, value)) as avg_gap,
    arrayZip(grid, timeSeriesCountToGrid(start, end, step, window)(timestamp, value)) as count_gap
FROM
(
    SELECT
        arrayJoin(arrayZip(
            [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(UInt32),
            [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32))) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
) FORMAT Vertical;

-- Equal timestamps collapse to the greatest value: 5 of these 7 samples remain, so sum = 15, avg = 3, count = 5.
SELECT 'duplicate timestamps collapse to the greatest value (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 125, 140, 155, 170, 170]) AS dup_ts,
    [1., 3, 0.5, 2, 5, 1, 4] AS dup_vals
SELECT
    timeSeriesSumToGrid(200, 200, 1, 200)(dup_ts, dup_vals)[1] = 15 AS sum_ok,
    timeSeriesAvgToGrid(200, 200, 1, 200)(dup_ts, dup_vals)[1] = 3 AS avg_ok,
    timeSeriesCountToGrid(200, 200, 1, 200)(dup_ts, dup_vals)[1] = 5 AS count_ok;

-- The window must not slide by subtracting the departed value: 1e18 + 2 + 3 + 5 - 1e18 is 0 in Float64.
-- Grid point 2 holds only the three small samples.
SELECT 'window sliding past a large value keeps the small ones (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [100, 110, 120, 130]) AS ts,
    [1e18, 2., 3, 5] AS vals
SELECT
    timeSeriesSumToGrid(100, 140, 40, 35)(ts, vals)[2] = 10 AS sum_ok,
    timeSeriesAvgToGrid(100, 140, 40, 35)(ts, vals)[2] = 10. / 3 AS avg_ok,
    timeSeriesCountToGrid(100, 140, 40, 35)(ts, vals)[2] = 3 AS count_ok;

-- Compensated summation keeps samples far below the running sum: a plain Float64 sum of 1e16 and four 1s
-- gives 1e16, the compensated one gives 1e16 + 4.
SELECT 'compensated sum and avg keep the small samples beside a large one (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [100, 110, 120, 130, 140]) AS ts,
    [1e16, 1., 1, 1, 1] AS vals
SELECT
    timeSeriesSumToGrid(140, 140, 1, 50)(ts, vals)[1] = 1e16 + 4 AS sum_compensated,
    timeSeriesAvgToGrid(140, 140, 1, 50)(ts, vals)[1] = (1e16 + 4) / 5 AS avg_compensated;

-- The result has the value type of the input, like the other grid functions.
SELECT toTypeName(timeSeriesSumToGrid(0, 0, 0, 0)(0::UInt32, 1::Float32));
SELECT toTypeName(timeSeriesAvgToGrid(0, 0, 0, 0)(0::UInt32, 1::Float32));
SELECT toTypeName(timeSeriesCountToGrid(0, 0, 0, 0)(0::UInt32, 1::Float32));
SELECT toTypeName(timeSeriesCountToGrid(0, 0, 0, 0)(0::UInt32, 1::Float64));

DROP TABLE ts_raw_data;
