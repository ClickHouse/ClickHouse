-- `Time64` -> `Time64`: a value outside the clock window (beyond 999:59:59) is checked like the numeric sources.
SET date_time_overflow_behavior = 'throw';
SELECT CAST(addSeconds(toTime64('00:00:00', 0), 4000000) AS Time64(3)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(addSeconds(toTime64('00:00:00', 0), 4000000), 'Time64(3)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrNull(addSeconds(toTime64('00:00:00', 0), 4000000), 'Time64(3)');
SELECT CAST(addSeconds(toTime64('00:00:00', 0), 100) AS Time64(3));

SET date_time_overflow_behavior = 'saturate';
SELECT CAST(addSeconds(toTime64('00:00:00', 0), 4000000) AS Time64(3)), CAST(addSeconds(toTime64('00:00:00', 0), -4000000) AS Time64(3));

SET date_time_overflow_behavior = 'ignore';
SELECT CAST(addSeconds(toTime64('00:00:00', 0), 4000000) AS Time64(3)), CAST(addSeconds(toTime64('00:00:00', 0), -4000000) AS Time64(3));

-- `NaN` in the `VALUES` expression fallback fails like `CAST` instead of becoming the column default.
DROP TABLE IF EXISTS t_dt64_nan;
DROP TABLE IF EXISTS t_t64_nan;
CREATE TABLE t_dt64_nan (d DateTime64(3, 'UTC'), x UInt8) ENGINE = Memory;
CREATE TABLE t_t64_nan (d Time64(3), x UInt8) ENGINE = Memory;
SET input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_dt64_nan VALUES (nan, 1); -- { error DECIMAL_OVERFLOW }
INSERT INTO t_t64_nan VALUES (nan, 1); -- { error DECIMAL_OVERFLOW }
SELECT count() FROM t_dt64_nan;
SELECT count() FROM t_t64_nan;
DROP TABLE t_dt64_nan;
DROP TABLE t_t64_nan;

-- A `WITH FILL` bound of a coarser `DateTime64` is converted to the type of the sort key under the setting.
SET date_time_overflow_behavior = 'throw';
SELECT d FROM (SELECT toDateTime64('2262-04-11 23:47:16', 9, 'UTC') AS d) ORDER BY d WITH FILL FROM toDateTime64('2299-12-31 00:00:00', 0, 'UTC') STEP toIntervalSecond(1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SET date_time_overflow_behavior = 'saturate';
SELECT d FROM (SELECT toDateTime64('2262-04-11 23:47:16', 9, 'UTC') AS d) ORDER BY d WITH FILL FROM toDateTime64('2299-12-31 00:00:00', 0, 'UTC') TO toDateTime64('2299-12-31 00:00:00', 0, 'UTC') STEP toIntervalSecond(1);
SELECT d FROM (SELECT toDateTime64('2020-01-01 00:00:02', 9, 'UTC') AS d) ORDER BY d WITH FILL FROM toDateTime64('2020-01-01 00:00:00', 0, 'UTC') STEP toIntervalSecond(1);
