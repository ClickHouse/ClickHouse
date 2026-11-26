-- Function `timeSeriesSliceSortedArray` returns the part of a sorted array of samples within a time interval.

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;

SELECT '-- UInt32 timestamps';
WITH [(100, 1.), (110, 2.), (120, 3.), (130, 4.)]::Array(Tuple(UInt32, Float64)) AS samples
SELECT timeSeriesSliceSortedArray(samples, 105, 120) AS inside,
       timeSeriesSliceSortedArray(samples, 100, 130) AS whole,
       timeSeriesSliceSortedArray(samples, 0, 1000) AS wider,
       timeSeriesSliceSortedArray(samples, 0, 99) AS before,
       timeSeriesSliceSortedArray(samples, 131, 1000) AS after,
       timeSeriesSliceSortedArray(samples, 120, 110) AS min_greater_than_max,
       timeSeriesSliceSortedArray(samples, 110, 110) AS single_point,
       timeSeriesSliceSortedArray([]::Array(Tuple(UInt32, Float64)), 0, 1000) AS empty_array
FORMAT Vertical;

SELECT '-- duplicate timestamps are all kept: the function does not deduplicate';
SELECT timeSeriesSliceSortedArray([(100, 1.), (110, 2.), (110, 3.), (120, 4.)]::Array(Tuple(UInt32, Float64)), 110, 110);

SELECT '-- DateTime and DateTime64 timestamps, bounds of other types are converted to the type of the timestamps';
SELECT timeSeriesSliceSortedArray([(toDateTime(100), 1.), (toDateTime(110), 2.), (toDateTime(120), 3.)], 105, 115);
WITH [('2025-06-01 00:00:00'::DateTime64(3), 1.), ('2025-06-01 00:00:30'::DateTime64(3), 2.), ('2025-06-01 00:01:00'::DateTime64(3), 3.)] AS samples
SELECT timeSeriesSliceSortedArray(samples, '2025-06-01 00:00:15'::DateTime64(3), '2025-06-01 00:01:00'::DateTime64(3)) AS datetime64_bounds,
       timeSeriesSliceSortedArray(samples, toDateTime('2025-06-01 00:00:30'), toDateTime('2025-06-01 00:00:59')) AS datetime_bounds,
       timeSeriesSliceSortedArray(samples, 1748736000, 1748736030) AS integer_bounds,
       timeSeriesSliceSortedArray(samples, 1748736000.5, 1748736059.999) AS fractional_bounds,
       timeSeriesSliceSortedArray(samples, '2025-06-01 00:00:30'::DateTime64(6), '2025-06-01 00:00:30.000001'::DateTime64(6)) AS other_scale_bounds
FORMAT Vertical;

SELECT '-- the names of the tuple elements are kept';
SELECT timeSeriesSliceSortedArray(CAST([(100, 1.), (110, 2.)], 'Array(Tuple(timestamp UInt32, value Float64))'), 105, 115) AS result, toTypeName(result);

SELECT '-- non-constant arrays and bounds';
SELECT number, timeSeriesSliceSortedArray(arrayMap(i -> (toUInt32(i * 10), toFloat64(i)), range(5)), number * 10, number * 10 + 15) FROM numbers(4);

SELECT '-- a constant bound together with a non-constant one';
SELECT number, timeSeriesSliceSortedArray(arrayMap(i -> (toUInt32(i * 10), toFloat64(i)), range(5)), 10, number * 10) FROM numbers(4);

SELECT '-- consecutive rows: slices before and after the interval, a partial slice, an empty array and a whole array';
SELECT number, timeSeriesSliceSortedArray(arrayMap(i -> (toUInt32(number * 100 + i * 10), toFloat64(i)), range(if(number = 2, 0, 3))), 105, 320) FROM numbers(5);

SELECT '-- every slice is a whole array';
SELECT number, timeSeriesSliceSortedArray(arrayMap(i -> (toUInt32(number * 100 + i * 10), toFloat64(i)), range(3)), 0, 1000) FROM numbers(3);

SELECT '-- a column of an aggregating table';
DROP TABLE IF EXISTS slice_test;
CREATE TABLE slice_test (id UInt8, samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))))
ENGINE = AggregatingMergeTree ORDER BY id;
-- The two rows inserted below must stay unmerged for the first SELECT.
SYSTEM STOP MERGES slice_test;
INSERT INTO slice_test VALUES (1, [('2025-06-01 00:00:00', 1.), ('2025-06-01 00:00:30', 2.), ('2025-06-01 00:01:00', 3.)]);
INSERT INTO slice_test VALUES (1, [('2025-06-01 00:00:45', 2.5)]);
SELECT timeSeriesSliceSortedArray(samples, '2025-06-01 00:00:30'::DateTime64(3), '2025-06-01 00:01:00'::DateTime64(3)) AS result, toTypeName(result) FROM slice_test ORDER BY result;
-- After a merge the rows are combined by `timeSeriesGroupArray`.
SYSTEM START MERGES slice_test;
OPTIMIZE TABLE slice_test FINAL;
SELECT timeSeriesSliceSortedArray(samples, '2025-06-01 00:00:30'::DateTime64(3), '2025-06-01 00:01:00'::DateTime64(3)) AS result, toTypeName(result) FROM slice_test ORDER BY result;
DROP TABLE slice_test;

SELECT '-- errors';
SELECT timeSeriesSliceSortedArray([1, 2, 3], 1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesSliceSortedArray([('a', 1.)], 1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesSliceSortedArray([(100, 1.)]::Array(Tuple(UInt32, Float64)), 'abc', 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesSliceSortedArray([(100, 1.)]::Array(Tuple(UInt32, Float64)), 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
