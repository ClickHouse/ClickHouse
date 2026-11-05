-- A `Date` / `Date32` / `DateTime` `WITH FILL FROM` / `TO` bound of a `DateTime64` sort key is converted with its own
-- type: its raw day number must not be taken as a number of seconds, and it honours `date_time_overflow_behavior`.

DROP TABLE IF EXISTS t_fill_dt64_date;
CREATE TABLE t_fill_dt64_date (dt DateTime64(3, 'UTC')) ENGINE = Memory;
INSERT INTO t_fill_dt64_date VALUES ('1970-01-03 00:00:00');

SELECT dt FROM t_fill_dt64_date ORDER BY dt WITH FILL FROM toDate32('1970-01-02') STEP toIntervalHour(12);
SELECT '--';
SELECT dt FROM t_fill_dt64_date ORDER BY dt WITH FILL FROM toDate('1970-01-02') TO toDate('1970-01-04') STEP toIntervalHour(12);
SELECT '--';
SELECT dt FROM t_fill_dt64_date ORDER BY dt WITH FILL FROM toDateTime('1970-01-02 12:00:00', 'UTC') STEP toIntervalHour(6);

DROP TABLE t_fill_dt64_date;

-- A `Date32` bound outside the window of a high-scale `DateTime64`.
CREATE TABLE t_fill_dt64_date (dt DateTime64(9, 'UTC')) ENGINE = Memory;
INSERT INTO t_fill_dt64_date VALUES ('2262-04-11 00:00:00');

SELECT '-- throw';
SELECT dt FROM t_fill_dt64_date ORDER BY dt WITH FILL TO toDate32('2299-12-31') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT '-- saturate';
SELECT count(), max(dt), min(dt) FROM (SELECT dt FROM t_fill_dt64_date ORDER BY dt DESC WITH FILL FROM toDate32('2299-12-31') STEP toIntervalHour(-1) SETTINGS date_time_overflow_behavior = 'saturate');

DROP TABLE t_fill_dt64_date;
