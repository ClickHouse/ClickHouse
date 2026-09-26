-- A numeric `WITH FILL FROM` / `TO` bound of a `DateTime64` sort key is materialized into the column, so it honours
-- `date_time_overflow_behavior` like `CAST` of the same constant, instead of producing ticks outside the calendar window.

DROP TABLE IF EXISTS t_fill_dt64;
CREATE TABLE t_fill_dt64 (dt DateTime64(0, 'UTC')) ENGINE = Memory;
INSERT INTO t_fill_dt64 VALUES (253402300790);

SELECT 'throw';
SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL FROM 253402300797 TO 253402300900 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL FROM 253402300800 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT dt FROM t_fill_dt64 ORDER BY dt DESC WITH FILL FROM 253402300797 TO -100000000000 STEP -1 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT 'saturate';
SELECT toUnixTimestamp64Second(dt) FROM (SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL FROM 253402300797 TO 253402300900 SETTINGS date_time_overflow_behavior = 'saturate');

SELECT 'ignore';
SELECT toUnixTimestamp64Second(dt) FROM (SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL FROM 253402300797 TO 253402300900 SETTINGS date_time_overflow_behavior = 'ignore');

SELECT 'in range';
SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL FROM 253402300787 TO 253402300790.5 SETTINGS date_time_overflow_behavior = 'throw';

DROP TABLE t_fill_dt64;

-- Nullable key and a fractional scale.
CREATE TABLE t_fill_dt64 (dt Nullable(DateTime64(3, 'UTC'))) ENGINE = Memory;
INSERT INTO t_fill_dt64 VALUES (0);
SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL FROM 1 TO 2.5 STEP 0.5 SETTINGS date_time_overflow_behavior = 'throw';
SELECT dt FROM t_fill_dt64 ORDER BY dt WITH FILL TO 9223372036855 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE t_fill_dt64;
