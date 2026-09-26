SET session_timezone = 'UTC';

-- The last day of the month or of the week can be past the `Date` maximum (2149-06-06);
-- it saturates there instead of wrapping around to 1970.

SELECT toLastDayOfMonth(toDate('2149-06-04')), toLastDayOfWeek(toDate('2149-06-02')), toLastDayOfWeek(toDate('2149-06-02'), 1);
SELECT toLastDayOfMonth(toDate('2149-06-06')), toLastDayOfWeek(toDate('2149-06-06'));

-- Dates whose result is in range are unaffected.
SELECT toLastDayOfMonth(toDate('2149-05-04')), toLastDayOfWeek(toDate('2149-05-25')), toLastDayOfWeek(toDate('2149-05-25'), 1);
SELECT toLastDayOfMonth(toDate('2020-02-03')), toLastDayOfWeek(toDate('2020-02-03')), toLastDayOfWeek(toDate('2020-02-03'), 1);
SELECT toLastDayOfMonth(toDate('1970-01-01')), toLastDayOfWeek(toDate('1970-01-01'));

DROP TABLE IF EXISTS t_last_day_max;
CREATE TABLE t_last_day_max (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_last_day_max SELECT toDate('2149-05-25') + number FROM numbers(13);

-- The functions are used for primary-key pruning, so a wrapped result dropped matching rows.
SELECT countIf(toLastDayOfWeek(d) = toDate('2149-05-31')), count() FROM t_last_day_max WHERE toLastDayOfWeek(d) = toDate('2149-05-31');
SELECT countIf(toLastDayOfWeek(d) = toDate('2149-06-06')), count() FROM t_last_day_max WHERE toLastDayOfWeek(d) = toDate('2149-06-06');
SELECT countIf(toLastDayOfMonth(d) = toDate('2149-05-31')), count() FROM t_last_day_max WHERE toLastDayOfMonth(d) = toDate('2149-05-31');
SELECT countIf(toLastDayOfMonth(d) = toDate('2149-06-06')), count() FROM t_last_day_max WHERE toLastDayOfMonth(d) = toDate('2149-06-06');

DROP TABLE t_last_day_max;
