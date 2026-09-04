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
    1734955380 AS start, 1734955680 AS end, 15 AS step, 300 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesMaxToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as max_5m,
    arrayZip(grid, timeSeriesMinToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as min_5m
FROM ts_raw_data FORMAT Vertical;

-- Check that -Merge returns the same result as a direct call (i.e. serialize/deserialize and the sliding
-- window's monoid combine agree with the non-windowed calculation).
CREATE TABLE ts_data_agg(k UInt64, max_agg AggregateFunction(timeSeriesMaxToGrid(1734955380, 1734955680, 15, 300), UInt32, Float64), min_agg AggregateFunction(timeSeriesMinToGrid(1734955380, 1734955680, 15, 300), UInt32, Float64)) ENGINE AggregatingMergeTree() ORDER BY k;

INSERT INTO ts_data_agg SELECT toUnixTimestamp(timestamp)%3,
    initializeAggregation('timeSeriesMaxToGridState(1734955380, 1734955680, 15, 300)', toUnixTimestamp(timestamp), value),
    initializeAggregation('timeSeriesMinToGridState(1734955380, 1734955680, 15, 300)', toUnixTimestamp(timestamp), value)
FROM ts_raw_data;

SELECT timeSeriesMaxToGridMerge(1734955380, 1734955680, 15, 300)(max_agg) FROM ts_data_agg;
SELECT timeSeriesMaxToGrid(1734955380, 1734955680, 15, 300)(toUnixTimestamp(timestamp), value) FROM ts_raw_data;

SELECT timeSeriesMinToGridMerge(1734955380, 1734955680, 15, 300)(min_agg) FROM ts_data_agg;
SELECT timeSeriesMinToGrid(1734955380, 1734955680, 15, 300)(toUnixTimestamp(timestamp), value) FROM ts_raw_data;

-- NaN handling: all 4 samples share timestamp 0 (the single grid point) so they land in the same bucket,
-- where the duplicate-timestamp rule collapses them: a NaN loses to any real value, so timestamp 0 keeps 5
-- -> 5 for both max and min.
SELECT timeSeriesMaxToGrid(0, 0, 0, 10)([0, 0, 0, 0]::Array(UInt32), [nan, nan, 5, nan]::Array(Float64));
SELECT timeSeriesMinToGrid(0, 0, 0, 10)([0, 0, 0, 0]::Array(UInt32), [nan, nan, 5, nan]::Array(Float64));

-- Duplicate timestamps: the `timeSeries*` family rule collapses a timestamp to the greatest value at it
-- before the over-time extremum is applied, so (0, 1) and (0, 2) yield 2 for min as well. The second pair
-- checks a displaced extremum: (0, 1) arrives after (1, 3) displaced (0, 5) as the minimum, and timestamp 0
-- still collapses to 5, so min is 3, not 1.
SELECT timeSeriesMaxToGrid(0, 0, 0, 10)([0, 0]::Array(UInt32), [1, 2]::Array(Float64));
SELECT timeSeriesMinToGrid(0, 0, 0, 10)([0, 0]::Array(UInt32), [1, 2]::Array(Float64));
SELECT timeSeriesMinToGrid(1, 1, 0, 10)([0, 1, 0]::Array(UInt32), [5, 3, 1]::Array(Float64));
SELECT timeSeriesMaxToGrid(1, 1, 0, 10)([0, 1, 0]::Array(UInt32), [5, 3, 1]::Array(Float64));

-- The same duplicate pair through the scalar-arguments path and through -Merge, where the two samples
-- live in different partial states.
CREATE TABLE ts_dup_data(timestamp DateTime64(3,'UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;
INSERT INTO ts_dup_data VALUES (toDateTime64(0, 3, 'UTC'), 1), (toDateTime64(0, 3, 'UTC'), 2);

SELECT timeSeriesMinToGrid(0, 0, 0, 10)(toUnixTimestamp(timestamp), value) FROM ts_dup_data;

CREATE TABLE ts_dup_agg(k UInt8, min_agg AggregateFunction(timeSeriesMinToGrid(0, 0, 0, 10), UInt32, Float64)) ENGINE AggregatingMergeTree() ORDER BY k;
INSERT INTO ts_dup_agg SELECT 0, initializeAggregation('timeSeriesMinToGridState(0, 0, 0, 10)', toUnixTimestamp(timestamp), value) FROM ts_dup_data;
SELECT timeSeriesMinToGridMerge(0, 0, 0, 10)(min_agg) FROM ts_dup_agg;
DROP TABLE ts_dup_agg;
DROP TABLE ts_dup_data;

DROP TABLE ts_raw_data;
DROP TABLE ts_data_agg;
