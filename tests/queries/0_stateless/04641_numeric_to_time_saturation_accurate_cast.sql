-- Conversion of a number to `Time` must saturate to the documented range [-999:59:59, 999:59:59]
-- for every numeric source type, and the accurate cast must reject the values that do not fit,
-- instead of silently saturating them, while keeping the negative values that `Time` supports.

SET use_legacy_to_time = 0;

-- Saturation of the stored value, not only of its text representation.
SELECT toInt32(toTime(toUInt8(200)));
SELECT toInt32(toTime(toUInt16(60000)));
SELECT toInt32(toTime(toUInt32(4000000)));
SELECT toInt32(toTime(toUInt64(4000000)));
SELECT toInt32(toTime(toUInt128(4000000)));
SELECT toInt32(toTime(toUInt256(4000000)));
SELECT toInt32(toTime(toInt32(4000000)));
SELECT toInt32(toTime(toInt32(-4000000)));
SELECT toInt32(toTime(toInt64(-4000000)));
SELECT toInt32(toTime(toInt128(4000000)));
SELECT toInt32(toTime(toInt256(-4000000)));
SELECT toInt32(toTime(toFloat64(4000000.5)));

-- The boundary values themselves are preserved.
SELECT toInt32(toTime(3599999)), toInt32(toTime(-3599999)), toInt32(toTime(-1)), toInt32(toTime(0));

-- The accurate cast rejects the values that `Time` cannot represent.
SELECT accurateCastOrNull(toUInt32(4000000), 'Time');
SELECT accurateCastOrNull(toInt32(-4000000), 'Time');
SELECT accurateCastOrNull(toInt128(4000000), 'Time');
SELECT accurateCastOrNull(toFloat64(4000000), 'Time');
SELECT accurateCastOrNull(toUInt64(9223372036854775808), 'Time');
SELECT accurateCastOrNull(nan, 'Time');
SELECT accurateCastOrNull(CAST('inf' AS Float32), 'Time');
SELECT accurateCastOrNull(toFloat64(-1.5), 'Time');
SELECT accurateCastOrNull(toFloat32(1.5), 'Time');
SELECT accurateCast(toUInt32(4000000), 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toInt32(-4000000), 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toFloat64(-1.5), 'Time'); -- { serverError CANNOT_CONVERT_TYPE }

-- ... and keeps the negative and out-of-`DateTime`-range values that it can represent.
SELECT accurateCast(toInt32(-1), 'Time'), accurateCastOrNull(toInt32(-1), 'Time');
SELECT accurateCast(materialize(toInt32(-3599999)), 'Time'), accurateCastOrNull(materialize(toInt32(-3599999)), 'Time');
SELECT accurateCast(toUInt32(3599999), 'Time'), accurateCastOrNull(toFloat64(-2), 'Time');

-- `Date32` follows the same contract as `Date`: a `UInt64` value above `Int64::max` wraps to a negative
-- `time_t`, which would escape the clamp, so it has to saturate to the upper boundary of the type.
SELECT toDate32(toUInt64(9223372036854775808), 'UTC');
SELECT toDate32(toUInt64(18446744073709551615), 'UTC');
SELECT toDate32(materialize(toUInt64(9223372036854775808)), 'UTC');
