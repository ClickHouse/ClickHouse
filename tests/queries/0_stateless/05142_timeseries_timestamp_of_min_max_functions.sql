SET enable_time_series_aggregate_functions = 1;

-- The dataset from the documentation examples: the gap between 140 and 190 leaves the windows
-- of grid points 150, 165, 180 without fresh samples.
SELECT 'array_args';
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesMinToGrid(90, 210, 15, 45)(timestamps, values);
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesMaxToGrid(90, 210, 15, 45)(timestamps, values);
-- Among equal values the latest timestamp wins: at grid point 210 the minimum 5 occurs at 190 and 200,
-- so ts_of_min is 200.
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesTimestampOfMinToGrid(90, 210, 15, 45)(timestamps, values);
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesTimestampOfMaxToGrid(90, 210, 15, 45)(timestamps, values);

-- The same dataset stored in a table with DateTime64 timestamps and scalar arguments.
DROP TABLE IF EXISTS ts_min_max_data;
CREATE TABLE ts_min_max_data(timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_min_max_data VALUES (110, 1), (120, 1), (130, 3), (140, 4), (190, 5), (200, 5), (210, 8), (220, 12), (230, 13);

SELECT 'scalar_args';
SELECT timeSeriesMinToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_min_max_data;
SELECT timeSeriesMaxToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_min_max_data;
SELECT timeSeriesTimestampOfMinToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_min_max_data;
SELECT timeSeriesTimestampOfMaxToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_min_max_data;

-- Partial aggregation: merging two -State halves must give the same result as the direct query.
SELECT 'state_merge';
SELECT timeSeriesTimestampOfMinToGridMerge(90, 210, 15, 45)(state)
FROM
(
    SELECT timeSeriesTimestampOfMinToGridState(90, 210, 15, 45)(timestamp, value) AS state
    FROM ts_min_max_data
    GROUP BY toUnixTimestamp64Milli(timestamp) % 2
);
SELECT timeSeriesTimestampOfMaxToGridMerge(90, 210, 15, 45)(state)
FROM
(
    SELECT timeSeriesTimestampOfMaxToGridState(90, 210, 15, 45)(timestamp, value) AS state
    FROM ts_min_max_data
    GROUP BY toUnixTimestamp64Milli(timestamp) % 2
);

DROP TABLE ts_min_max_data;

-- Timestamps with a fractional part are returned in seconds with the fraction kept.
SELECT 'fractional_timestamps';
DROP TABLE IF EXISTS ts_fractional_data;
CREATE TABLE ts_fractional_data(timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_fractional_data VALUES (110.25, 1), (120.75, 2);
SELECT
    timeSeriesTimestampOfMinToGrid(121, 121, 0, 20)(timestamp, value),
    timeSeriesTimestampOfMaxToGrid(121, 121, 0, 20)(timestamp, value)
FROM ts_fractional_data;
DROP TABLE ts_fractional_data;

-- Samples with the same timestamp are deduplicated keeping the largest value: at timestamp 100
-- only the value 5 remains, so the minimum at grid point 100 is 5, not 2.
SELECT 'dedup';
WITH
    [100, 100, 110]::Array(DateTime) AS timestamps,
    [5, 2, 3]::Array(Float64) AS values
SELECT
    timeSeriesMinToGrid(100, 110, 10, 20)(timestamps, values),
    timeSeriesMaxToGrid(100, 110, 10, 20)(timestamps, values),
    timeSeriesTimestampOfMinToGrid(100, 110, 10, 20)(timestamps, values),
    timeSeriesTimestampOfMaxToGrid(100, 110, 10, 20)(timestamps, values);

-- A NaN loses to any real value, so NaN is returned only when every sample in the window is NaN;
-- ts_of_* then returns the timestamp of the NaN sample.
SELECT 'nan';
WITH
    [100, 110, 120]::Array(DateTime) AS timestamps,
    [nan, 2, nan]::Array(Float64) AS values
SELECT
    timeSeriesMinToGrid(100, 120, 10, 15)(timestamps, values),
    timeSeriesMaxToGrid(100, 120, 10, 15)(timestamps, values),
    timeSeriesTimestampOfMinToGrid(100, 120, 10, 15)(timestamps, values),
    timeSeriesTimestampOfMaxToGrid(100, 120, 10, 15)(timestamps, values);

-- A window of 64 buckets forces the two-stacks strategy. The values equal the timestamps, so within
-- the window (t - 64, t] the minimum is t - 63 and the maximum is t.
SELECT 'two_stacks';
SELECT
    timeSeriesMinToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1)),
    timeSeriesMaxToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1)),
    timeSeriesTimestampOfMinToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1)),
    timeSeriesTimestampOfMaxToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1))
FROM numbers(110);

-- min/max return the value type, while ts_of_* always returns Float64 because the result
-- is a timestamp in seconds.
SELECT 'result_types';
WITH
    [100, 110]::Array(DateTime) AS timestamps,
    [1, 2]::Array(Float32) AS values
SELECT
    toTypeName(timeSeriesMinToGrid(100, 110, 10, 20)(timestamps, values)),
    toTypeName(timeSeriesMaxToGrid(100, 110, 10, 20)(timestamps, values)),
    toTypeName(timeSeriesTimestampOfMinToGrid(100, 110, 10, 20)(timestamps, values)),
    toTypeName(timeSeriesTimestampOfMaxToGrid(100, 110, 10, 20)(timestamps, values));

-- ts_of_* timestamps near the current epoch must not lose precision with Float32 values
-- (a Float32 near 1.7e9 has a ~128-second ulp).
SELECT 'float32_ts_precision';
WITH
    [1734955421, 1734955436]::Array(DateTime) AS timestamps,
    [1, 2]::Array(Float32) AS values
SELECT timeSeriesTimestampOfMaxToGrid(1734955421, 1734955436, 15, 30)(timestamps, values);
