-- `accurateCast` / `accurateCastOrNull` from `Date32` to `DateTime64` must follow the accurate-cast contract:
-- a day whose midnight does not fit the `Int64` ticks of the result type (`9999-12-31` at scale 9 - the scale-9
-- range ends at 2262-04-11) throws or yields NULL regardless of `date_time_overflow_behavior`, instead of
-- saturating to the boundary like the plain `CAST` does. The plain `CAST` keeps following the setting.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_date32_accurate;
CREATE TABLE t_date32_accurate (d Date32) ENGINE = Memory;
INSERT INTO t_date32_accurate VALUES ('9999-12-31'), ('2262-04-11'), ('2262-04-12'), ('1677-09-21'), ('1677-09-22'), ('0000-01-01'), ('2024-01-01');

SELECT '-- accurateCastOrNull, materialized column, scale 9: unrepresentable days are NULL in every overflow mode';
SELECT d, accurateCastOrNull(d, 'DateTime64(9)') FROM t_date32_accurate ORDER BY d SETTINGS date_time_overflow_behavior = 'ignore';
SELECT d, accurateCastOrNull(d, 'DateTime64(9)') FROM t_date32_accurate ORDER BY d SETTINGS date_time_overflow_behavior = 'saturate';
SELECT d, accurateCastOrNull(d, 'DateTime64(9)') FROM t_date32_accurate ORDER BY d SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- accurateCastOrNull, constant';
SELECT accurateCastOrNull(toDate32('9999-12-31'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(toDate32('9999-12-31'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(toDate32('0000-01-01'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- accurateCastOrNull at other scales: scale 3 holds every Date32 day, scale 8 ends at 4892-10-07';
SELECT accurateCastOrNull(toDate32('9999-12-31'), 'DateTime64(3)'), accurateCastOrNull(toDate32('0000-01-01'), 'DateTime64(3)');
SELECT accurateCastOrNull(toDate32('9999-12-31'), 'DateTime64(8)'), accurateCastOrNull(toDate32('0000-01-01'), 'DateTime64(8)');
SELECT accurateCastOrNull(toDate32('4892-10-07'), 'DateTime64(8)'), accurateCastOrNull(toDate32('4892-10-08'), 'DateTime64(8)');

SELECT '-- accurateCast throws regardless of the overflow mode';
SELECT accurateCast(toDate32('9999-12-31'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(toDate32('9999-12-31'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(toDate32('9999-12-31'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(d, 'DateTime64(9)') FROM t_date32_accurate SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(toDate32('2262-04-11'), 'DateTime64(9)'), accurateCast(toDate32('1677-09-22'), 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- the plain CAST still follows the setting';
SELECT CAST(d, 'DateTime64(9)') FROM t_date32_accurate WHERE d = '9999-12-31' SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(d, 'DateTime64(9)') FROM t_date32_accurate WHERE d = '9999-12-31' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

DROP TABLE t_date32_accurate;
