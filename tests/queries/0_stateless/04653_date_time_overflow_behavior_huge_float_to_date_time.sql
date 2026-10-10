-- Huge floating-point sources must be clamped in the floating-point domain: casting them straight to `time_t`
-- is undefined behaviour, so `saturate` used to return an architecture-dependent value instead of the bound.

SET session_timezone = 'UTC';

SELECT '-- saturate, constants';
SET date_time_overflow_behavior = 'saturate';
SELECT toDate(1e300::Float64), toDate(1e30::Float32), toDate(1e30::BFloat16);
SELECT toDate32(1e300::Float64), toDate32(1e30::Float32);
SELECT toDateTime(1e300::Float64), toDateTime(1e30::Float32);
SELECT toTime(1e300::Float64), toTime(-1e300::Float64), toTime(1e30::Float32);
SELECT toDateTime64(1e300::Float64, 3), toTime64(1e300::Float64, 3);

SELECT '-- saturate, columns';
SELECT toDate(x), toDate32(x), toDateTime(x), toTime(x), toDateTime64(x, 3) FROM (SELECT materialize(1e300::Float64) AS x);
SELECT toDate(x), toDate32(x), toDateTime(x), toTime(x) FROM (SELECT materialize(1e30::Float32) AS x);
SELECT toTime(x) FROM (SELECT materialize(-1e300::Float64) AS x);

SELECT '-- saturate, NaN';
-- `toDate`, `toDateTime` and `toTime` reject non-finite sources regardless of the overflow mode,
-- while `toDate32` saturates NaN to the minimum representable day.
SELECT toDate(nan::Float64); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate32(nan::Float64);
SELECT toDateTime(nan::Float64); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime(nan::Float64); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate32(x) FROM (SELECT materialize(nan::Float64) AS x);
SELECT toDateTime(x) FROM (SELECT materialize(nan::Float64) AS x); -- { serverError CANNOT_CONVERT_TYPE }
-- `DateTime64` and `Time64` follow the same contract: a non-finite value must not slip past the clamps
-- into `convertToDecimal` (which would raise `DECIMAL_OVERFLOW`), it is rejected up front in every mode.
SELECT toDateTime64(nan::Float64, 3); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime64(nan::Float64, 3); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime64(inf::Float64, 3); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime64(-inf::Float64, 3); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime64(x, 3) FROM (SELECT materialize(nan::Float64) AS x); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime64(x, 3) FROM (SELECT materialize(nan::Float32) AS x); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- ignore';
SET date_time_overflow_behavior = 'ignore';
SELECT toDate(1e300::Float64), toDateTime(1e300::Float64), toTime(1e300::Float64);

SELECT '-- throw';
SET date_time_overflow_behavior = 'throw';
SELECT toDate(1e300::Float64); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate32(1e300::Float64); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(1e300::Float64); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime(1e300::Float64); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime(-1e300::Float64); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate(nan::Float64); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate32(nan::Float64); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(nan::Float64); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime(nan::Float64); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(x) FROM (SELECT materialize(1e300::Float64) AS x); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(x) FROM (SELECT materialize(nan::Float64) AS x); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime64(nan::Float64, 3); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime64(nan::Float64, 3); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime64(1e300::Float64, 3); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime64(1e300::Float64, 3); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- values in range are unaffected';
SET date_time_overflow_behavior = 'saturate';
SELECT toDate(0.9::Float32), toDate(100000::Float64), toDate32(100000::Float64);
SELECT toDateTime(1000.5::Float64), toTime(3600.5::Float64), toTime(-3600.5::Float64);
