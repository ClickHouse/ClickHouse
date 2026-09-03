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

-- Empty windows (staleness window narrower than the grid step) must yield NULL, not 0, for a grid point with
-- no samples: the gap between timestamps 140 and 190 leaves grid points 150, 165 and 180 without any sample.
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

-- A repeated timestamp is a sample of its own: the bucket keeps duplicate timestamps, so sum/avg/count fold
-- every occurrence. For these 7 samples (two timestamps repeated) on a single-point grid whose window (0,200]
-- covers them all: sum = 16.5, avg = 16.5/7, count = 7 - not the deduplicated 15, 3 and 5.
SELECT 'duplicate timestamps keep their multiplicity (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 125, 140, 155, 170, 170]) AS dup_ts,
    [1., 3, 0.5, 2, 5, 1, 4] AS dup_vals
SELECT
    timeSeriesSumToGrid(200, 200, 1, 200)(dup_ts, dup_vals)[1] = 16.5 AS sum_ok,
    timeSeriesAvgToGrid(200, 200, 1, 200)(dup_ts, dup_vals)[1] = 16.5 / 7 AS avg_ok,
    timeSeriesCountToGrid(200, 200, 1, 200)(dup_ts, dup_vals)[1] = 7 AS count_ok;

-- Sliding the window past a large value must not be done by subtracting it: in Float64 the small values
-- still in the window are cancelled by the big one leaving (1e18 + 2 + 3 + 5 - 1e18 is 0). sum/avg
-- therefore recompute the window; count is exact under subtraction and keeps the O(1) running sum.
-- Grid point 1 holds only the large sample, grid point 2 only the three small ones.
SELECT 'window sliding past a large value keeps the small ones (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [100, 110, 120, 130]) AS ts,
    [1e18, 2., 3, 5] AS vals
SELECT
    timeSeriesSumToGrid(100, 140, 40, 35)(ts, vals)[2] = 10 AS sum_ok,
    timeSeriesAvgToGrid(100, 140, 40, 35)(ts, vals)[2] = 10. / 3 AS avg_ok,
    timeSeriesCountToGrid(100, 140, 40, 35)(ts, vals)[2] = 3 AS count_ok;

-- avg_over_time sums with Kahan-Babuska-Neumaier compensation, so samples far smaller than the running
-- sum are not swallowed by it: 1e16 has an ulp of 2, so a plain Float64 sum drops every one of the four
-- 1s that follow and reports 1e16/5. The compensated sum keeps them and reports (1e16 + 4)/5.
SELECT 'compensated avg keeps the small samples beside a large one (1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [100, 110, 120, 130, 140]) AS ts,
    [1e16, 1., 1, 1, 1] AS vals
SELECT timeSeriesAvgToGrid(140, 140, 1, 50)(ts, vals)[1] = (1e16 + 4) / 5 AS avg_compensated;

-- Neither avg_over_time nor sum_over_time may combine buckets in the two-stacks queue's order: regrouping
-- needs an associative merge, and neither the compensated summary nor a plain Float64 addition is one, so a
-- dense window whose rounding error depends on the merge order would report a value depending on the queue
-- state. Here 25 buckets per window (the old two-stacks forcing threshold) hold values summing to exactly 1
-- when combined in time order, and the dense multi-point grid must agree with the single-point grid over the
-- same window, which combines its samples sequentially.
SELECT 'avg combines buckets in time order on a dense window (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), range(100, 126)) AS ts,
    [1e16, -1e-16, -1e16, 1e-16, 1e-16, 1e-16, -1e-16, -1., 1., 1e16, 1., 1., -1., 1e16, -1e16, 1e16, -1., 1e-16, 1., -1e16, -1e-16, 1e-16, 1., 1., -1., -1.] AS vals
SELECT
    timeSeriesAvgToGrid(100, 125, 1, 25)(ts, vals)[26] = timeSeriesAvgToGrid(125, 125, 1, 25)(ts, vals)[1] AS matches_sequential_reference,
    timeSeriesAvgToGrid(125, 125, 1, 25)(ts, vals)[1] = 0.04 AS reference_value;

-- How the grid slices a window into buckets is an internal detail, so the same samples over the same
-- window must average identically whatever the step is. Merging rounded per-bucket totals did not
-- satisfy that: with these values 25 one-second buckets and 5 five-second buckets cover exactly the
-- samples at 100..124 and disagree in the last digit (…99.4 against …99.5). Both grids below land on
-- 124 with a 25-second window, so they differ only in bucketization.
SELECT 'avg is independent of how the window is bucketed:';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), range(100, 125)) AS ts,
    [1e-16, -1e16, -1e-16, 3., 1., -1e16, 1., -1e16, -1e-16, -1., -1e-16, -1e-16, 1e16, 1e16, 1e-16, 1., 1., -1e-16, 3., -1e16, -1., 3., 3., -1e16, 1e16] AS vals
SELECT
    timeSeriesAvgToGrid(124, 124, 1, 25)(ts, vals)[1] = timeSeriesAvgToGrid(104, 124, 5, 25)(ts, vals)[5] AS same_regardless_of_bucketing,
    toString(timeSeriesAvgToGrid(124, 124, 1, 25)(ts, vals)[1]) AS folded_in_timestamp_order;

-- The same for sum_over_time at 20 buckets per window, the current forcing threshold. Combined in time
-- order the window sums to 1e16 + 2; a queue holding the first `1` apart from the rest folds the other two
-- together first and loses both, reporting 1e16.
SELECT 'sum combines buckets in time order on a dense window:';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), range(100, 121)) AS ts,
    [0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 1., 1., 1e16] AS vals
SELECT
    timeSeriesSumToGrid(100, 120, 1, 20)(ts, vals)[21] = timeSeriesSumToGrid(120, 120, 1, 20)(ts, vals)[1] AS matches_sequential_reference,
    timeSeriesSumToGrid(120, 120, 1, 20)(ts, vals)[1] = 1e16 + 2 AS reference_value;

-- `timeSeriesCountToGrid` must return an exact `Float64` count regardless of the input value type.
-- Inheriting `Float32` would round counts above 16777216.
SELECT toTypeName(timeSeriesCountToGrid(0, 0, 0, 0)(0::UInt32, 1::Float32));
SELECT toTypeName(timeSeriesCountToGrid(0, 0, 0, 0)(0::UInt32, 1::Float64));

-- sum_over_time/avg_over_time still project to the input value type, unlike count_over_time
SELECT toTypeName(timeSeriesSumToGrid(0, 0, 0, 0)(0::UInt32, 1::Float32));
SELECT toTypeName(timeSeriesSumToGrid(0, 0, 0, 0)(0::UInt32, 1::Float64));
SELECT toTypeName(timeSeriesAvgToGrid(0, 0, 0, 0)(0::UInt32, 1::Float32));
SELECT toTypeName(timeSeriesAvgToGrid(0, 0, 0, 0)(0::UInt32, 1::Float64));

DROP TABLE ts_raw_data;
