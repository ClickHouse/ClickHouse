SET allow_experimental_time_series_aggregate_functions = 1;

SELECT 'The greatest value wins on duplicate timestamps:';

SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [3, 5]::Array(Float64));
SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [5, 3]::Array(Float64));

SELECT 'NaN loses to a real value:';

SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [3, nan]::Array(Float64));
SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [nan, 3]::Array(Float64));
SELECT timeSeriesGroupArray([90, 95, 95, 98]::Array(UInt32), [1, nan, 5, 2]::Array(Float64));

SELECT 'NaN survives when all values at the timestamp are NaN:';

SELECT timeSeriesGroupArray([95, 95]::Array(UInt32), [nan, nan]::Array(Float64));
SELECT timeSeriesGroupArray([90, 95, 95]::Array(UInt32), [1, nan, nan]::Array(Float64));

-- Of two NaNs the greater bit pattern wins, so the Prometheus stale marker 0x7ff0000000000002 loses to a quiet NaN in either order.
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), timeSeriesGroupArray([95, 95]::Array(UInt32), [reinterpretAsFloat64(0x7ff0000000000002), nan]));
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), timeSeriesGroupArray([95, 95]::Array(UInt32), [nan, reinterpretAsFloat64(0x7ff0000000000002)]));

-- Of +0 and -0 the +0 wins in either order. Its bits are checked: `hex` prints +0 as '00', -0 would be '8000000000000000'.
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), timeSeriesGroupArray([95, 95]::Array(UInt32), [0., -0.]::Array(Float64)));
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), timeSeriesGroupArray([95, 95]::Array(UInt32), [-0., 0.]::Array(Float64)));

-- A state saved by earlier versions kept the stale marker that came after a NaN. It keeps it,
-- and merging it with a new state at the same timestamp applies the rule above in either order.
WITH
    CAST(unhex('010001000000000000005F000000020000000000F07F'), 'AggregateFunction(timeSeriesGroupArray, UInt32, Float64)') AS saved,
    timeSeriesGroupArrayState(95::UInt32, nan::Float64) AS new
SELECT
    arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), finalizeAggregation(saved)),
    arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), arrayReduce('timeSeriesGroupArrayMerge', [saved, new])),
    arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), arrayReduce('timeSeriesGroupArrayMerge', [new, saved]));
