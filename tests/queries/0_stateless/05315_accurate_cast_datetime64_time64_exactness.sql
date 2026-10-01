-- `accurateCast` / `accurateCastOrNull` to `DateTime64` / `Time64` must reject a source that loses precision
-- (a fractional part finer than the target scale), not only a source that is out of range.
-- `DateTime64 -> Time64` takes the local seconds-of-day of the source, also for the accurate casts.

SET session_timezone = 'UTC';

SELECT '-- floating-point sources';
SELECT accurateCast(1.1::Float64, 'DateTime64(0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(materialize(1.1::Float64), 'DateTime64(0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(1.1::Float64, 'DateTime64(0)');
SELECT accurateCastOrNull(3600.0001::Float64, 'Time64(3)');
SELECT accurateCastOrNull(materialize(3600.0001::Float64), 'Time64(3)');
SELECT accurateCast(3600.0001::Float64, 'Time64(3)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1.5::Float64, 'DateTime64(0)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1.5::Float64, 'DateTime64(1)'), accurateCastOrNull(1.5::Float32, 'Time64(3)');
SELECT accurateCast(1.1::Float64, 'DateTime64(1)'), accurateCast(2.0::Float64, 'DateTime64(0)');
SELECT accurateCastOrNull(x, 'DateTime64(1)') FROM (SELECT materialize(arrayJoin([0.5, 0.25, 1.0]::Array(Float64))) AS x);

SELECT '-- decimal sources';
SELECT accurateCast(toDecimal64(1.2345, 4), 'DateTime64(3)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(toDecimal64(1.2345, 4), 'DateTime64(3)');
SELECT accurateCastOrNull(toDecimal64(1.2340, 4), 'DateTime64(3)');
SELECT accurateCastOrNull(toDecimal256('1.25', 40), 'Time64(1)'), accurateCastOrNull(toDecimal256('1.5', 40), 'Time64(1)');
SELECT accurateCastOrNull(toDateTime64('2024-01-01 00:00:00.1234', 4, 'UTC'), 'DateTime64(3)');
SELECT accurateCastOrNull(toDateTime64('2024-01-01 00:00:00.1230', 4, 'UTC'), 'DateTime64(3)');
SELECT accurateCast(materialize(toDateTime64('2024-01-01 00:00:00.1234', 4, 'UTC')), 'DateTime64(3)'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- DateTime64 to Time64 is the time of day';
SELECT accurateCast(toDateTime64('1970-01-02 00:00:00', 0, 'UTC'), 'Time64(0)');
SELECT accurateCastOrNull(toDateTime64('1970-01-02 00:00:00', 0, 'UTC'), 'Time64(0)');
SELECT accurateCast(toDateTime64('2024-06-01 12:34:56.789', 3, 'UTC'), 'Time64(3)');
SELECT accurateCastOrNull(materialize(toDateTime64('2024-06-01 12:34:56.789', 3, 'UTC')), 'Time64(3)');

SELECT '-- narrowing the scale of the last second';
SELECT accurateCastOrNull(toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC'), 'DateTime64(1)'), accurateCastOrNull(toDateTime64('9999-12-31 23:59:59.900', 3, 'UTC'), 'DateTime64(1)');
