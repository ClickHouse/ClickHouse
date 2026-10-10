-- An `accurateCast` / `accurateCastOrNull` of a non-integral floating-point number to `Date` or
-- `DateTime` must reject the value instead of silently truncating it, matching the generic numeric
-- accurate-cast contract (a non-integral `1.5` is rejected by `accurateCast(1.5, 'Int32')` as well).

-- A numeric source is interpreted as a unix timestamp in the session time zone,
-- so pin it to make the values deterministic.
SET session_timezone = 'UTC';

-- Non-integral floating-point values are rejected.
SELECT accurateCastOrNull(toFloat64(1.5), 'DateTime');
SELECT accurateCastOrNull(toFloat32(1.5), 'DateTime');
SELECT accurateCastOrNull(toBFloat16(1.5), 'DateTime');
SELECT accurateCastOrNull(toFloat64(1704067200.5), 'DateTime');
SELECT accurateCastOrNull(materialize(toFloat64(1704067200.5)), 'DateTime');
SELECT accurateCastOrNull(toFloat64(1.5), 'Date');
SELECT accurateCastOrNull(toFloat32(19723.5), 'Date');
SELECT accurateCast(toFloat64(1.5), 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toFloat64(19723.5), 'Date'); -- { serverError CANNOT_CONVERT_TYPE }

-- Integral floating-point values are still accepted.
SELECT accurateCast(toFloat64(1704067200), 'DateTime'), accurateCastOrNull(toFloat64(1704067200), 'DateTime');
SELECT accurateCast(toFloat64(0), 'DateTime'), accurateCastOrNull(toFloat32(0), 'DateTime');

-- The ordinary conversion still truncates the fractional part.
SELECT toDateTime(toFloat64(1704067200.5));
