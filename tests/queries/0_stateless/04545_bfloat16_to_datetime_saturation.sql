-- Conversion of BFloat16 to DateTime must saturate to the range boundaries,
-- like Float32 and Float64 do, instead of relying on the platform-specific
-- result of an out-of-range float-to-integer cast (which wraps around on x86-64
-- but saturates on AArch64).

-- In-range value.
SELECT toDateTime32(CAST(1000000 AS BFloat16), 'UTC');
SELECT toDateTime(CAST(1000000 AS BFloat16), 'UTC');

-- Value above the DateTime range: saturate to the upper boundary.
SELECT toDateTime32(CAST(1e10 AS BFloat16), 'UTC');
SELECT toDateTime(CAST(1e10 AS BFloat16), 'UTC');

-- Negative value: saturate to the lower boundary.
SELECT toDateTime32(CAST(-100 AS BFloat16), 'UTC');
SELECT toDateTime(CAST(-100 AS BFloat16), 'UTC');

-- Numeric inputs saturate regardless of date_time_overflow_behavior.
SELECT toDateTime32(CAST(1e10 AS BFloat16), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDateTime32(CAST(-100 AS BFloat16), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';

-- Very large finite values, above the range of time_t after the cast to Float64,
-- must also saturate instead of hitting an undefined float-to-integer cast.
SELECT toDateTime32(CAST(1e38 AS BFloat16), 'UTC');
SELECT toDateTime32(CAST(-1e38 AS BFloat16), 'UTC');
SELECT toDateTime32(CAST(1e300 AS Float64), 'UTC');
SELECT toDateTime32(CAST(-1e300 AS Float64), 'UTC');

-- Non-finite values cannot be converted and throw, like they do for integer targets.
SELECT toDateTime32(CAST('nan' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime32(CAST('inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime32(CAST('-inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime(nan, 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDateTime(CAST('inf' AS Float32), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }

-- The toDate float path follows the same contract: non-finite values throw,
-- huge finite values saturate to the upper boundary instead of hitting an
-- undefined float-to-integer cast.
SELECT toDate(CAST(100 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e10 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e38 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e300 AS Float64), 'UTC');
SELECT toDate(CAST(-100 AS BFloat16), 'UTC');
SELECT toDate(CAST(1e300 AS Float64), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDate(CAST('nan' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(CAST('inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(CAST('-inf' AS BFloat16), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(nan, 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toDate(CAST('inf' AS Float32), 'UTC'); -- { serverError CANNOT_CONVERT_TYPE }

-- The same branch handles conversion of BFloat16 to Time: saturate to the Time range.
SET use_legacy_to_time = 0;
SELECT toTime(CAST(1e10 AS BFloat16));
SELECT toTime(CAST(-1e10 AS BFloat16));
SELECT toTime(CAST(1e38 AS BFloat16));
SELECT toTime(CAST(-1e38 AS BFloat16));
SELECT toTime(CAST('nan' AS BFloat16)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime(CAST('inf' AS BFloat16)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toTime(CAST('-inf' AS BFloat16)); -- { serverError CANNOT_CONVERT_TYPE }

-- UInt64 values above Int64::max must also saturate: the conversion to time_t
-- wraps them to a negative number, which would otherwise escape the clamp.
SELECT toDateTime32(toUInt64(9223372036854775808), 'UTC');
SELECT toDateTime32(toUInt64(18446744073709551615), 'UTC');
SELECT toDateTime(toUInt64(9223372036854775808), 'UTC');
SELECT toDateTime32(toUInt64(9223372036854775808), 'UTC') SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDate(toUInt64(9223372036854775808), 'UTC');
SELECT toDate(toUInt64(18446744073709551615), 'UTC');
SELECT toTime(toUInt64(9223372036854775808));
SELECT toTime(toUInt64(18446744073709551615));

-- Wide integers take the same path for Date and Date32; a value above Int64::max
-- wraps when it is cast to time_t, so it has to be clamped in the source domain.
SELECT toDate(toUInt128(9223372036854775808), 'UTC');
SELECT toDate(toUInt128('340282366920938463463374607431768211455'), 'UTC');
SELECT toDate(toInt128(9223372036854775808), 'UTC');
SELECT toDate(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), 'UTC');
SELECT toDate(toInt256(9223372036854775808), 'UTC');
SELECT toDate(toInt128(-9223372036854775808), 'UTC');
SELECT toDate32(toUInt64(9223372036854775808), 'UTC');
SELECT toDate32(toUInt128(9223372036854775808), 'UTC');
SELECT toDate32(toUInt128('340282366920938463463374607431768211455'), 'UTC');
SELECT toDate32(toInt128(9223372036854775808), 'UTC');
SELECT toDate32(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), 'UTC');
SELECT toDate32(toInt256(9223372036854775808), 'UTC');
SELECT toDate32(toInt128(-9223372036854775808), 'UTC');
