-- Element counts stored in the states of the time series aggregate functions come from untrusted data,
-- so deserialization must not allocate memory for the claimed number of elements.

SET allow_experimental_time_series_aggregate_functions = 1;

-- A valid state of `timeSeriesGroupArray`: format version, number of elements, the timestamps, the values.
SELECT finalizeAggregation(CAST(unhex('0100' || '0200000000000000' || 'E803000000000000' || 'D007000000000000' || '000000000000F83F' || '0000000000000440') AS AggregateFunction(timeSeriesGroupArray, DateTime64(3, 'UTC'), Float64)));

-- The number of elements is so big that its size in bytes overflows.
SELECT finalizeAggregation(CAST(unhex('0100' || '0000000000000010') AS AggregateFunction(timeSeriesGroupArray, DateTime64(3, 'UTC'), Float64))); -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('0100' || 'FFFFFFFFFFFFFFFF') AS AggregateFunction(timeSeriesGroupArray, DateTime64(3, 'UTC'), Float64))); -- { serverError CANNOT_READ_ALL_DATA }

-- A state with more elements than are reserved before reading, so the array grows while it is read.
SELECT length(finalizeAggregation(CAST(CAST(state AS String) AS AggregateFunction(timeSeriesGroupArray, DateTime64(3, 'UTC'), Float64))))
FROM (SELECT timeSeriesGroupArrayState(toDateTime64(number, 3, 'UTC'), number::Float64) AS state FROM numbers(10000));

-- The functions that keep samples per bucket, such as `timeSeriesChangesToGrid`, store the number of samples too.
SELECT length(finalizeAggregation(CAST(state AS AggregateFunction(timeSeriesChangesToGrid(10, 120, 10, 70), UInt32, Float64))))
FROM (SELECT CAST(timeSeriesChangesToGridState(10, 120, 10, 70)(toUInt32(55), toFloat64(1)) AS String) AS state);

-- The same state with the number of samples of its only bucket (the 8 bytes after the format version,
-- the bucket count, the number of buckets and the bucket index) replaced with a huge value.
SELECT length(finalizeAggregation(CAST(substring(state, 1, 26) || unhex('FFFFFFFFFFFFFFFF') || substring(state, 35) AS AggregateFunction(timeSeriesChangesToGrid(10, 120, 10, 70), UInt32, Float64))))
FROM (SELECT CAST(timeSeriesChangesToGridState(10, 120, 10, 70)(toUInt32(55), toFloat64(1)) AS String) AS state); -- { serverError CANNOT_READ_ALL_DATA }
