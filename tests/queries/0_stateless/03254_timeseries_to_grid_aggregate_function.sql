CREATE TABLE ts_data (timestamp DateTime('UTC'), value Float64) ENGINE=MergeTree() ORDER BY tuple();

WITH [11, 57, 71, 88, 89, 101, 127, 135, 151] as timestamps
INSERT INTO ts_data SELECT ts::DateTime64 as timestamp, ts + 10000 as value FROM (SELECT arrayJoin(timestamps) as ts);

WITH [102, 104, 112, 113, 120] as timestamps
INSERT INTO ts_data SELECT ts::DateTime64 as timestamp, ts + 10000 as value FROM (SELECT arrayJoin(timestamps) as ts);

SET allow_experimental_ts_to_grid_aggregate_function = 1;

SELECT 'Original data (ts, val):';
SELECT groupArraySorted(30)((toUnixTimestamp(timestamp), value)) FROM ts_data;

SELECT 'timeSeriesResampleToGridWithStaleness(100, 200, 10, 15):';

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
       timeSeriesResampleToGridWithStaleness(begin, end, step_sec, staleness_sec)(timestamp, value)
   ) as a
FROM ts_data;

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
       timeSeriesResampleToGridWithStaleness(begin_ts, end_ts, step_sec, staleness_sec)(timestamp, value)
   ) as b
FROM ts_data;

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
       timeSeriesResampleToGridWithStaleness(begin_ts, end_ts, step_sec, staleness_sec)(timestamp::DateTime64(3, 'UTC'), value::Float32)
   ) as c
FROM ts_data;

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
       timeSeriesResampleToGridWithStaleness(begin_ts::DateTime64(2, 'UTC'), end_ts::DateTime64(1, 'UTC'), step_sec::Decimal(6,2), staleness_sec::Decimal(18,3))(timestamp::DateTime64(3, 'UTC'), value::Float32)
   ) as d
FROM ts_data;

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
       timeSeriesResampleToGridWithStaleness(begin_ts, end_ts::DateTime64(3, 'UTC'), step_sec::Decimal(6,2), staleness_sec)(timestamp::DateTime64(6, 'UTC'), value)
   ) as e
FROM ts_data;

-- AggregatingMergeTree Table to test (de)serialization of timeSeriesResampleToGridWithStaleness state
CREATE TABLE ts_data_agg(k UInt64, agg AggregateFunction(timeSeriesResampleToGridWithStaleness(100, 200, 10, 15), DateTime('UTC'), Float64)) ENGINE AggregatingMergeTree() ORDER BY k;

-- Insert the data splitting it into several pieces
INSERT INTO ts_data_agg SELECT toUnixTimestamp(timestamp)%3, initializeAggregation('timeSeriesResampleToGridWithStalenessState(100, 200, 10, 15)', timestamp, value) FROM ts_data;

SELECT k, finalizeAggregation(agg) FROM ts_data_agg FINAL ORDER BY k;

-- Check that -Merge returns the same result as the result form original table
SELECT timeSeriesResampleToGridWithStaleness(100, 200, 10, 15)(timestamp, value) FROM ts_data;
SELECT timeSeriesResampleToGridWithStalenessMerge(100, 200, 10, 15)(agg) FROM ts_data_agg;

-- Check various data types for parameters and arguments
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50)(timestamp, value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50)(timestamp::DateTime64(2,'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(100::Int32, 150::UInt16, 15::Decimal(10,2), 50)(timestamp::DateTime64(3, 'UTC'), value::Float32) AS res FROM ts_data;

-- When the timestamp argument is DateTime64, parameters can also be floats and strings
-- containing numbers, durations (like '15s' or '1m') or date-time text
SELECT timeSeriesResampleToGridWithStaleness(100.0, 150.0, 15.0, 50.0)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness('100', '150', '15s', '50s')(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness('1970-01-01 00:01:40', '1970-01-01 00:02:30', 15, '1m')(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data SETTINGS session_timezone = 'UTC';

-- The window '1m' means 60 seconds: the sample at 151 is still fresh for the grid point 210
SELECT timeSeriesResampleToGridWithStaleness(100, 210, 10, 60)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(100, 210, 10, '1m')(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data;

-- Decimal and float parameters keep their fractional part: start 100.5 shifts the whole grid,
-- so it ends at 140.5 (the next point 150.5 would be beyond the end)
SELECT timeSeriesResampleToGridWithStaleness(toDecimal32(100.5, 1), 150, 10, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(100.5, 150, 10, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStaleness(100, 100, 15, 50)(timestamp::DateTime64(3, 'UTC'), value::Float32) AS res FROM ts_data;
SELECT timeSeriesResampleToGridWithStalenessIf(100, 150, 15, 50)(timestamp, value, value%2==0) AS res FROM ts_data;

-- Subsecond step and window parameters
select timeSeriesResampleToGridWithStaleness(
    '2025-06-01 12:00:00.300'::DateTime64(3, 'UTC'),
    '2025-06-01 12:00:00.900'::DateTime64(3, 'UTC'),
    '0.300'::Decimal64(3),
    '0.500'::Decimal64(3))
  (['2025-06-01 12:00:00.011', '2025-06-01 12:00:00.768']::Array(DateTime64(3, 'UTC')),
   [10, 20]::Array(Float64));

SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50)(timestamp, value::Decimal(10,3)) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50)(timestamp, value::Int64) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50)(timestamp, value::String) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50)(timestamp, value::DateTime) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT timeSeriesResampleToGridWithStaleness(100::Float64, 150, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150::Float32, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15::Float32, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, 50::Float64)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- When the timestamp argument is DateTime64, parameters which cannot be parsed are rejected
SELECT timeSeriesResampleToGridWithStaleness('abc', 150, 15, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesResampleToGridWithStaleness([100], 150, 15, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesResampleToGridWithStaleness(inf, 150, 15, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesResampleToGridWithStaleness('1970-01-01 00:01:40junk', 150, 15, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesResampleToGridWithStaleness('1970-13-01 00:01:40', 150, 15, 50)(timestamp::DateTime64(3, 'UTC'), value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }

SELECT timeSeriesResampleToGridWithStaleness(-100, 150, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, -150, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, -15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 15, -50)(timestamp, value) AS res FROM ts_data; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT timeSeriesResampleToGridWithStaleness(200, 100, 15, 50)(timestamp, value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesResampleToGridWithStaleness(100, 150, 0, 50)(timestamp, value) AS res FROM ts_data; -- { serverError BAD_ARGUMENTS }
