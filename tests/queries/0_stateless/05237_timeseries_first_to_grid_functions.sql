SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

-- The dataset from the documentation examples: the gap between 140 and 190 leaves the windows
-- of grid points 150, 165, 180 without fresh samples when the window is 30 seconds.
-- The first sample of the window (t - 45, t] is compared with the last one.
SELECT 'array_args';
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesFirstToGrid(90, 210, 15, 45)(timestamps, values);
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesLastToGrid(90, 210, 15, 45)(timestamps, values);
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesTimestampOfFirstToGrid(90, 210, 15, 45)(timestamps, values);
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesTimestampOfLastToGrid(90, 210, 15, 45)(timestamps, values);

-- With the 30-second window the grid point 180 has an empty window, so the queue of in-window buckets
-- gets emptied and refilled.
SELECT 'empty_window_in_the_middle';
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float64) AS values
SELECT timeSeriesFirstToGrid(90, 210, 15, 30)(timestamps, values), timeSeriesTimestampOfFirstToGrid(90, 210, 15, 30)(timestamps, values);

-- The same dataset stored in a table with DateTime64 timestamps and scalar arguments.
DROP TABLE IF EXISTS ts_first_data;
CREATE TABLE ts_first_data(timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_first_data VALUES (110, 1), (120, 1), (130, 3), (140, 4), (190, 5), (200, 5), (210, 8), (220, 12), (230, 13);

SELECT 'scalar_args';
SELECT timeSeriesFirstToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_first_data;
SELECT timeSeriesTimestampOfFirstToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_first_data;

-- Samples passed as an array of (timestamp, value) pairs.
SELECT 'array_of_pairs';
SELECT timeSeriesFirstToGrid(90, 210, 15, 45)(samples)
FROM (SELECT groupArray((timestamp, value)) AS samples FROM ts_first_data);
SELECT timeSeriesTimestampOfFirstToGrid(90, 210, 15, 45)(samples)
FROM (SELECT groupArray((timestamp, value)) AS samples FROM ts_first_data);

-- Partial aggregation: merging two -State halves must give the same result as the direct query.
SELECT 'state_merge';
SELECT timeSeriesFirstToGridMerge(90, 210, 15, 45)(state)
FROM
(
    SELECT timeSeriesFirstToGridState(90, 210, 15, 45)(timestamp, value) AS state
    FROM ts_first_data
    GROUP BY toUnixTimestamp64Milli(timestamp) % 2
);
SELECT timeSeriesTimestampOfFirstToGridMerge(90, 210, 15, 45)(state)
FROM
(
    SELECT timeSeriesTimestampOfFirstToGridState(90, 210, 15, 45)(timestamp, value) AS state
    FROM ts_first_data
    GROUP BY toUnixTimestamp64Milli(timestamp) % 2
);

-- AggregatingMergeTree table to test (de)serialization of the state.
DROP TABLE IF EXISTS ts_first_agg;
CREATE TABLE ts_first_agg(k UInt64, first_agg AggregateFunction(timeSeriesFirstToGrid(90, 210, 15, 45), DateTime64(3, 'UTC'), Float64), ts_of_first_agg AggregateFunction(timeSeriesTimestampOfFirstToGrid(90, 210, 15, 45), DateTime64(3, 'UTC'), Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO ts_first_agg SELECT toUnixTimestamp64Milli(timestamp) % 3,
    initializeAggregation('timeSeriesFirstToGridState(90, 210, 15, 45)', timestamp, value),
    initializeAggregation('timeSeriesTimestampOfFirstToGridState(90, 210, 15, 45)', timestamp, value)
FROM ts_first_data;
DETACH TABLE ts_first_agg;
ATTACH TABLE ts_first_agg;
SELECT timeSeriesFirstToGridMerge(90, 210, 15, 45)(first_agg), timeSeriesTimestampOfFirstToGridMerge(90, 210, 15, 45)(ts_of_first_agg) FROM ts_first_agg;
DROP TABLE ts_first_agg;

DROP TABLE ts_first_data;

-- Timestamps with a fractional part are returned with the fraction kept.
SELECT 'fractional_timestamps';
WITH
    [110.25, 120.75]::Array(DateTime64(3, 'UTC')) AS timestamps,
    [1, 2]::Array(Float64) AS values
SELECT timeSeriesFirstToGrid(121, 121, 0, 20)(timestamps, values), timeSeriesTimestampOfFirstToGrid(121, 121, 0, 20)(timestamps, values);

-- Samples with the same timestamp are deduplicated keeping the largest value: at timestamp 100
-- only the value 5 remains.
SELECT 'dedup';
WITH
    [100, 100, 110]::Array(DateTime) AS timestamps,
    [2, 5, 3]::Array(Float64) AS values
SELECT
    timeSeriesFirstToGrid(100, 110, 10, 20)(timestamps, values),
    timeSeriesTimestampOfFirstToGrid(100, 110, 10, 20)(timestamps, values);

-- A NaN sample is returned when it is the first one in the window; at a duplicated timestamp a NaN loses
-- to any real value.
SELECT 'nan';
WITH
    [100, 110, 120]::Array(DateTime) AS timestamps,
    [nan, 2, nan]::Array(Float64) AS values
SELECT
    timeSeriesFirstToGrid(100, 120, 10, 15)(timestamps, values),
    timeSeriesTimestampOfFirstToGrid(100, 120, 10, 15)(timestamps, values);
WITH
    [100, 100]::Array(DateTime) AS timestamps,
    [nan, 2]::Array(Float64) AS values
SELECT
    timeSeriesFirstToGrid(100, 100, 0, 15)(timestamps, values),
    timeSeriesTimestampOfFirstToGrid(100, 100, 0, 15)(timestamps, values);

-- The -If combinator and Nullable arguments: the sample at 110 is excluded by the condition or hidden by a NULL,
-- so the window (105, 120] starts with the sample at 120.
SELECT 'if_and_nullable';
SELECT
    timeSeriesFirstToGridIf(100, 120, 10, 15)(timestamp, value, value != 2),
    timeSeriesTimestampOfFirstToGridIf(100, 120, 10, 15)(timestamp, value, value != 2)
FROM (SELECT arrayJoin([(100, 1.), (110, 2.), (120, 3.)]) AS sample, sample.1::DateTime AS timestamp, sample.2 AS value);
SELECT
    timeSeriesFirstToGrid(100, 120, 10, 15)(if(value = 2, NULL, timestamp), value),
    timeSeriesTimestampOfFirstToGrid(100, 120, 10, 15)(timestamp, if(value = 2, NULL, value))
FROM (SELECT arrayJoin([(100, 1.), (110, 2.), (120, 3.)]) AS sample, sample.1::DateTime AS timestamp, sample.2 AS value);

-- A step larger than the window: some buckets are never in any window and are skipped.
SELECT 'step_larger_than_window';
WITH
    [95, 100, 112, 116, 125]::Array(DateTime) AS timestamps,
    [1, 2, 3, 4, 5]::Array(Float64) AS values
SELECT
    timeSeriesFirstToGrid(100, 130, 15, 10)(timestamps, values),
    timeSeriesTimestampOfFirstToGrid(100, 130, 15, 10)(timestamps, values);

-- A window of 64 buckets: the values equal the timestamps, so within the window (t - 64, t]
-- the first sample is t - 63.
SELECT 'many_buckets_in_window';
SELECT
    timeSeriesFirstToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1)),
    timeSeriesTimestampOfFirstToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1))
FROM numbers(110);

-- timeSeriesFirstToGrid returns the value type, while timeSeriesTimestampOfFirstToGrid returns the type of the timestamps.
SELECT 'result_types';
WITH
    [100, 110]::Array(DateTime) AS timestamps,
    [1, 2]::Array(Float32) AS values
SELECT
    toTypeName(timeSeriesFirstToGrid(100, 110, 10, 20)(timestamps, values)),
    toTypeName(timeSeriesTimestampOfFirstToGrid(100, 110, 10, 20)(timestamps, values));

-- Wrong number of parameters.
SELECT timeSeriesFirstToGrid(100, 110, 10)([100]::Array(DateTime), [1]::Array(Float64)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesTimestampOfFirstToGrid(100, 110, 10)([100]::Array(DateTime), [1]::Array(Float64)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
