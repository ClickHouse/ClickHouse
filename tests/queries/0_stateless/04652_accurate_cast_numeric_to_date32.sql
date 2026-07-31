-- A numeric `accurateCast` / `accurateCastOrNull` to `Date32` must reject the values that the type
-- cannot represent, instead of silently saturating them to `1900-01-01` or `2299-12-31` the way the
-- ordinary `toDate32` does. The representable window is [-getDayNumOffsetEpoch(), MAX_DATE32_TIMESTAMP],
-- because a number is read either as an extended day number or as a unix timestamp.

-- A numeric source that is read as a unix timestamp is interpreted in the session time zone,
-- so pin it to make the boundary values deterministic.
SET session_timezone = 'UTC';

-- Oversized unsigned integers.
SELECT accurateCastOrNull(toUInt64(18446744073709551615), 'Date32');
SELECT accurateCastOrNull(toUInt64(9223372036854775808), 'Date32');
SELECT accurateCastOrNull(toUInt128(18446744073709551615), 'Date32');
SELECT accurateCastOrNull(toUInt256(18446744073709551615), 'Date32');
SELECT accurateCastOrNull(materialize(toUInt64(18446744073709551615)), 'Date32');
SELECT accurateCast(toUInt64(18446744073709551615), 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }

-- Oversized and non-finite floating-point values.
SELECT accurateCastOrNull(nan, 'Date32');
SELECT accurateCastOrNull(CAST('inf' AS Float32), 'Date32');
SELECT accurateCastOrNull(CAST('-inf' AS Float64), 'Date32');
SELECT accurateCastOrNull(1e30, 'Date32');
SELECT accurateCast(nan, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e30, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }

-- Non-integral floating-point values are not representable and are rejected too.
SELECT accurateCastOrNull(toFloat64(1704067200.5), 'Date32');
SELECT accurateCastOrNull(materialize(toFloat64(1704067200.5)), 'Date32');
SELECT accurateCast(toFloat64(1704067200.5), 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }

-- Values below the first representable day number.
SELECT accurateCastOrNull(toInt32(-25568), 'Date32');
SELECT accurateCastOrNull(toInt64(-10413792000), 'Date32');
SELECT accurateCastOrNull(toInt128(-10413792000), 'Date32');
SELECT accurateCastOrNull(toInt256(-10413792000), 'Date32');
SELECT accurateCastOrNull(-25567.5, 'Date32');
SELECT accurateCast(toInt32(-25568), 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }

-- The boundary values themselves and the ordinary values are preserved.
SELECT accurateCast(toInt32(-25567), 'Date32'), accurateCastOrNull(toInt32(-25567), 'Date32');
SELECT accurateCast(toInt64(10413791999), 'Date32'), accurateCastOrNull(toInt64(10413791999), 'Date32');
SELECT accurateCast(toUInt32(0), 'Date32'), accurateCastOrNull(toInt32(19723), 'Date32');
SELECT accurateCast(materialize(toInt64(1704067200)), 'Date32'), accurateCastOrNull(materialize(toFloat64(1704067200)), 'Date32');

-- The ordinary conversion still saturates, and it is unaffected by the accurate-cast check.
SELECT toDate32(toUInt64(18446744073709551615), 'UTC'), toDate32(toInt32(-25568)), toDate32(toInt64(10413792000), 'UTC');
