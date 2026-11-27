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

SET enable_time_series_aggregate_functions = 1;

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

-- ts_of_max_over_time / ts_of_min_over_time: the timestamp (in seconds) of the extremum. Among equal values the
-- latest sample wins: the maximum 8 at 1734955646 and 1734955661 gives 1734955661 for the grid point 1734955665,
-- and the minimum 0 at 1734955421 and 1734955436 gives 1734955436 once both are in the window.
SELECT timeSeriesTimestampOfMaxToGrid(1734955380, 1734955680, 15, 300)(toUnixTimestamp(timestamp), value) FROM ts_raw_data;
SELECT timeSeriesTimestampOfMinToGrid(1734955380, 1734955680, 15, 300)(toUnixTimestamp(timestamp), value) FROM ts_raw_data;

-- -Merge returns the same as the direct call.
CREATE TABLE ts_data_agg_ts_of(k UInt64, max_agg AggregateFunction(timeSeriesTimestampOfMaxToGrid(1734955380, 1734955680, 15, 300), UInt32, Float64), min_agg AggregateFunction(timeSeriesTimestampOfMinToGrid(1734955380, 1734955680, 15, 300), UInt32, Float64)) ENGINE AggregatingMergeTree() ORDER BY k;

INSERT INTO ts_data_agg_ts_of SELECT toUnixTimestamp(timestamp)%3,
    initializeAggregation('timeSeriesTimestampOfMaxToGridState(1734955380, 1734955680, 15, 300)', toUnixTimestamp(timestamp), value),
    initializeAggregation('timeSeriesTimestampOfMinToGridState(1734955380, 1734955680, 15, 300)', toUnixTimestamp(timestamp), value)
FROM ts_raw_data;

SELECT timeSeriesTimestampOfMaxToGridMerge(1734955380, 1734955680, 15, 300)(max_agg) FROM ts_data_agg_ts_of;
SELECT timeSeriesTimestampOfMinToGridMerge(1734955380, 1734955680, 15, 300)(min_agg) FROM ts_data_agg_ts_of;
DROP TABLE ts_data_agg_ts_of;

-- With DateTime64 timestamps the returned seconds keep the fractional part.
SELECT
    timeSeriesTimestampOfMaxToGrid(1734955680, 1734955680, 0, 300)(timestamp, value),
    timeSeriesTimestampOfMinToGrid(1734955680, 1734955680, 0, 300)(timestamp, value)
FROM ts_raw_data;

-- NaN handling: at a duplicated timestamp a NaN loses to the real value 5, so the timestamp 0 is returned;
-- across timestamps a real value beats a NaN too; an all-NaN window returns the timestamp of the latest sample.
SELECT timeSeriesTimestampOfMaxToGrid(0, 0, 0, 10)([0, 0, 0, 0]::Array(UInt32), [nan, nan, 5, nan]::Array(Float64)),
       timeSeriesTimestampOfMinToGrid(0, 0, 0, 10)([0, 0, 0, 0]::Array(UInt32), [nan, nan, 5, nan]::Array(Float64));
SELECT timeSeriesTimestampOfMaxToGrid(2, 2, 0, 10)([0, 1, 2]::Array(UInt32), [nan, 5, nan]::Array(Float64)),
       timeSeriesTimestampOfMinToGrid(2, 2, 0, 10)([0, 1, 2]::Array(UInt32), [nan, 5, nan]::Array(Float64));
SELECT timeSeriesTimestampOfMaxToGrid(2, 2, 0, 10)([0, 1, 2]::Array(UInt32), [nan, nan, nan]::Array(Float64)),
       timeSeriesTimestampOfMinToGrid(2, 2, 0, 10)([0, 1, 2]::Array(UInt32), [nan, nan, nan]::Array(Float64));

-- Duplicate timestamps: timestamp 0 collapses to 5, so the maximum is at 0 and the minimum (3) is at 1.
SELECT timeSeriesTimestampOfMaxToGrid(1, 1, 0, 10)([0, 1, 0]::Array(UInt32), [5, 3, 1]::Array(Float64)),
       timeSeriesTimestampOfMinToGrid(1, 1, 0, 10)([0, 1, 0]::Array(UInt32), [5, 3, 1]::Array(Float64));

-- Equal values: the latest timestamp wins for both functions.
SELECT timeSeriesTimestampOfMaxToGrid(2, 2, 0, 10)([0, 1, 2]::Array(UInt32), [7, 7, 7]::Array(Float64)),
       timeSeriesTimestampOfMinToGrid(2, 2, 0, 10)([0, 1, 2]::Array(UInt32), [7, 7, 7]::Array(Float64));

-- NaN handling on the two-stacks path (a window of 50 buckets forces it) and through split -State merges:
-- a real value beats a NaN, so the window (52, 102] with samples NaN, 5, NaN gives 5 at timestamp 101.
DROP TABLE IF EXISTS ts_nan_data;
CREATE TABLE ts_nan_data(timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_nan_data VALUES (100, nan), (101, 5), (102, nan);
SELECT
    timeSeriesMaxToGrid(102, 103, 1, 50)(timestamp, value),
    timeSeriesMinToGrid(102, 103, 1, 50)(timestamp, value),
    timeSeriesTimestampOfMaxToGrid(102, 103, 1, 50)(timestamp, value),
    timeSeriesTimestampOfMinToGrid(102, 103, 1, 50)(timestamp, value)
FROM ts_nan_data;
SELECT
    timeSeriesMaxToGridMerge(102, 103, 1, 50)(max_state),
    timeSeriesTimestampOfMinToGridMerge(102, 103, 1, 50)(min_timestamp_state)
FROM
(
    SELECT
        timeSeriesMaxToGridState(102, 103, 1, 50)(timestamp, value) AS max_state,
        timeSeriesTimestampOfMinToGridState(102, 103, 1, 50)(timestamp, value) AS min_timestamp_state
    FROM ts_nan_data
    GROUP BY toUnixTimestamp(timestamp) % 2
);
DROP TABLE ts_nan_data;

-- An all-NaN window returns NaN, and ts_of_* return the timestamp of its latest sample.
DROP TABLE IF EXISTS ts_all_nan;
CREATE TABLE ts_all_nan(timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_all_nan VALUES (100, nan), (101, nan);
SELECT
    timeSeriesMaxToGrid(101, 102, 1, 50)(timestamp, value),
    timeSeriesMinToGrid(101, 102, 1, 50)(timestamp, value),
    timeSeriesTimestampOfMaxToGrid(101, 102, 1, 50)(timestamp, value),
    timeSeriesTimestampOfMinToGrid(101, 102, 1, 50)(timestamp, value)
FROM ts_all_nan;
SELECT timeSeriesTimestampOfMaxToGridMerge(101, 102, 1, 50)(s)
FROM
(
    SELECT timeSeriesTimestampOfMaxToGridState(101, 102, 1, 50)(timestamp, value) AS s
    FROM ts_all_nan
    GROUP BY toUnixTimestamp(timestamp) % 2
);
DROP TABLE ts_all_nan;

DROP TABLE ts_raw_data;
DROP TABLE ts_data_agg;
