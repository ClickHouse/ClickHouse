-- `accurateCast` / `accurateCastOrNull` from a decimal to `DateTime64` / `Time64` must honour the accurate-cast
-- contract: a value that is not representable in the result type throws or yields NULL, instead of the
-- `DECIMAL_OVERFLOW` raised by the plain rescaling path. The `date_time_overflow_behavior` setting must not
-- change any of it.

SET session_timezone = 'UTC';

SELECT '-- accurateCastOrNull, constant';
SELECT accurateCastOrNull(toDecimal64(300000000000, 0), 'DateTime64(9)');
SELECT accurateCastOrNull(toDecimal64(-300000000000, 0), 'DateTime64(9)');
SELECT accurateCastOrNull(toDecimal128(1000000000000, 0), 'DateTime64(0)');
SELECT accurateCastOrNull(toDecimal64(300000000000, 0), 'Time64(9)');
SELECT accurateCastOrNull(toDecimal256(-300000000000, 0), 'Time64(3)');

SELECT '-- accurateCastOrNull, in range';
SELECT accurateCastOrNull(toDecimal64(1.5, 1), 'DateTime64(3)');
SELECT accurateCastOrNull(toDecimal32(3599.5, 1), 'Time64(3)');
SELECT accurateCastOrNull(toDecimal128(1735689600, 0), 'DateTime64(6)');

SELECT '-- accurateCastOrNull, materialized column';
SELECT accurateCastOrNull(v, 'DateTime64(9)')
FROM (SELECT materialize(toDecimal64(number * 300000000000, 0)) AS v FROM numbers(3))
ORDER BY ALL;

SELECT '-- accurateCastOrNull, DateTime64 source that does not fit a higher scale';
SELECT accurateCastOrNull(toDateTime64('2300-01-01 00:00:00', 0, 'UTC'), 'DateTime64(9)');
SELECT accurateCastOrNull(materialize(toDateTime64('2300-01-01 00:00:00', 0, 'UTC')), 'DateTime64(9)');

SELECT '-- accurateCast throws';
SELECT accurateCast(toDecimal64(300000000000, 0), 'DateTime64(9)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(materialize(toDecimal64(300000000000, 0)), 'Time64(9)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(toDecimal64(1.5, 1), 'DateTime64(3)');

SELECT '-- the setting does not change the accurate-cast contract';
SELECT accurateCastOrNull(toDecimal64(300000000000, 0), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(toDecimal64(300000000000, 0), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(toDecimal64(300000000000, 0), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCast(toDecimal64(300000000000, 0), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
