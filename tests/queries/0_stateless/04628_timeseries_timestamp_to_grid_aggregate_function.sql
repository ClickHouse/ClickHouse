-- Tests timeSeriesTimestampToGrid(), the aggregate function backing PromQL's timestamp() function: like
-- timeSeriesResampleToGridWithStaleness() (see 03254_timeseries_to_grid_aggregate_function_sparse.sql) it keeps
-- the most recent sample within each grid point's window, but it returns that sample's own TIMESTAMP instead of
-- its value.

CREATE TABLE ts_data (timestamp DateTime('UTC'), value Float64) ENGINE=MergeTree() ORDER BY tuple();

WITH [11, 57, 71, 88, 89, 101, 127, 135, 151] as timestamps
INSERT INTO ts_data SELECT ts::DateTime64 as timestamp, ts + 10000 as value FROM (SELECT arrayJoin(timestamps) as ts);

WITH [102, 104, 112, 113, 120] as timestamps
INSERT INTO ts_data SELECT ts::DateTime64 as timestamp, ts + 10000 as value FROM (SELECT arrayJoin(timestamps) as ts);

SET allow_experimental_ts_to_grid_aggregate_function = 1;

SELECT 'Original data (ts, val):';
SELECT groupArraySorted(30)((toUnixTimestamp(timestamp), value)) FROM ts_data;

SELECT 'timeSeriesTimestampToGrid(100, 200, 10, 15):';

WITH
    100 as begin,
    200 as end,
    10 as step_sec,
    15 as staleness_sec,
    CAST(begin as DateTime('UTC')) as begin_ts,
    CAST(end as DateTime('UTC')) as end_ts,
    range(begin, end+step_sec, step_sec) as grid
SELECT
   arrayZip(
       grid,
       timeSeriesTimestampToGrid(begin, end, step_sec, staleness_sec)(timestamp, value)
   ) as a
FROM ts_data;

-- Same grid, but with DateTime64 timestamps and Float32 values.
WITH
    100 as begin,
    200 as end,
    10 as step_sec,
    15 as staleness_sec,
    CAST(begin as DateTime('UTC')) as begin_ts,
    CAST(end as DateTime('UTC')) as end_ts,
    range(begin, end+step_sec, step_sec) as grid
SELECT
   arrayZip(
       grid,
       timeSeriesTimestampToGrid(begin_ts, end_ts, step_sec, staleness_sec)(timestamp::DateTime64(3, 'UTC'), value::Float32)
   ) as b
FROM ts_data;

-- Test for returning multiple rows in batch
SELECT intDiv(toUnixTimestamp(timestamp), 130)*130 as fake_key, timeSeriesTimestampToGrid(100, 200, 10, 15)(timestamp, value) FROM ts_data GROUP BY fake_key ORDER BY fake_key;

-- AggregatingMergeTree table to test (de)serialization of the timeSeriesTimestampToGrid state
CREATE TABLE ts_data_agg(k UInt64, agg AggregateFunction(timeSeriesTimestampToGrid(100, 200, 10, 15), DateTime('UTC'), Float64)) ENGINE AggregatingMergeTree() ORDER BY k;

-- Insert the data splitting it into several pieces
INSERT INTO ts_data_agg SELECT toUnixTimestamp(timestamp)%3, initializeAggregation('timeSeriesTimestampToGridState(100, 200, 10, 15)', timestamp, value) FROM ts_data;

SELECT k, finalizeAggregation(agg) FROM ts_data_agg FINAL ORDER BY k;

-- Reload table and check that the data is the same (i.e. serialize-deserialize worked correctly)
DETACH TABLE ts_data_agg;
ATTACH TABLE ts_data_agg;
SELECT k, finalizeAggregation(agg) FROM ts_data_agg FINAL ORDER BY k;

-- Check that -Merge returns the same result as the result from the original table
SELECT timeSeriesTimestampToGrid(100, 200, 10, 15)(timestamp, value) FROM ts_data;
SELECT timeSeriesTimestampToGridMerge(100, 200, 10, 15)(agg) FROM ts_data_agg;

-- Check various data types for parameters and arguments
SELECT timeSeriesTimestampToGrid(100, 150, 15, 50)(timestamp, value) AS res FROM ts_data;
SELECT timeSeriesTimestampToGrid(100, 150, 15, 50)(timestamp::DateTime64(2,'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesTimestampToGrid(100::Int32, 150::UInt16, 15::Decimal(10,2), 50)(timestamp::DateTime64(3, 'UTC'), value::Float32) AS res FROM ts_data;
SELECT timeSeriesTimestampToGrid(100, 100, 15, 50)(timestamp::DateTime64(3, 'UTC'), value::Float32) AS res FROM ts_data;
SELECT timeSeriesTimestampToGridIf(100, 150, 15, 50)(timestamp, value, value%2==0) AS res FROM ts_data;

-- Test with Nullable timestamps and values: a NULL in either column excludes the sample from aggregation.
SELECT timeSeriesTimestampToGrid(100, 150, 15, 50)(if (value < 10120, Null, timestamp), value::Float32) AS res FROM ts_data;
SELECT timeSeriesTimestampToGrid(100, 150, 15, 50)(timestamp, if (value < 10120, Null, value)) AS res FROM ts_data;

SELECT timeSeriesTimestampToGrid(100, 150, 15, 50)(timestamp, value::Decimal(10,3)) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesTimestampToGrid(100, 150, 15, 50)(timestamp, value::String) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesTimestampToGrid(100, 150::Float32, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT timeSeriesTimestampToGrid(200, 100, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesTimestampToGrid(100, 150, 0, 50)(timestamp, value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
