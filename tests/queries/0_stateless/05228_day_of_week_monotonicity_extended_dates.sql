SET session_timezone = 'UTC';

-- `toDayOfWeek` is monotonic only inside a single day, so the factor transform that decides it has to
-- keep the day number exactly. `toDate` narrows the day number to `UInt16`, which aliases it modulo
-- 65536 on the extended carriers: `1970-01-01` (day 0) and `2149-06-07` (day 65536) got the same
-- factor, a key range spanning them was reported monotonic even though the weekday wraps inside it,
-- and the granule holding a matching Monday was pruned away.

DROP TABLE IF EXISTS t_day_of_week_date32;

CREATE TABLE t_day_of_week_date32 (d Date32) ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_day_of_week_date32 VALUES ('1970-01-01'), ('1970-01-05'), ('2149-06-07');

SELECT toDayOfWeek(d) FROM t_day_of_week_date32 ORDER BY d;
SELECT count() FROM t_day_of_week_date32 WHERE toDayOfWeek(d) = 1;

DROP TABLE t_day_of_week_date32;

-- The same aliasing is reachable from a `DateTime64` key, whose day numbers also leave the `Date` range.

DROP TABLE IF EXISTS t_day_of_week_datetime64;

CREATE TABLE t_day_of_week_datetime64 (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_day_of_week_datetime64 VALUES ('1970-01-01 00:00:00.000'), ('1970-01-05 12:00:00.000'), ('2149-06-07 00:00:00.000');

SELECT toDayOfWeek(dt) FROM t_day_of_week_datetime64 ORDER BY dt;
SELECT count() FROM t_day_of_week_datetime64 WHERE toDayOfWeek(dt) = 1;

DROP TABLE t_day_of_week_datetime64;

-- A range that stays inside one day is still monotonic, so the primary key remains usable there.

DROP TABLE IF EXISTS t_day_of_week_single_day;

CREATE TABLE t_day_of_week_single_day (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;

INSERT INTO t_day_of_week_single_day SELECT toDateTime('2026-08-03 00:00:00', 'UTC') + (3600 * number) FROM numbers(24);

SELECT count() FROM t_day_of_week_single_day WHERE toDayOfWeek(dt) = 1 SETTINGS force_primary_key = 1;
SELECT count() FROM t_day_of_week_single_day WHERE toDayOfWeek(dt) = 2 SETTINGS force_primary_key = 1;

DROP TABLE t_day_of_week_single_day;

-- The factor also has to round a `DateTime64` down. `toDate` reads the whole part of the decimal
-- components, which truncates towards zero, so a pre-epoch value at 23:59:59.5 got the factor of the
-- *next* day and a range from it into that day looked monotonic. `toDayOfWeek` runs from Sunday to
-- Monday there, and the monotonicity claim is an increasing one, so the derived range was empty and
-- every row was pruned.

DROP TABLE IF EXISTS t_day_of_week_negative_fraction;

CREATE TABLE t_day_of_week_negative_fraction (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_day_of_week_negative_fraction VALUES ('1969-12-28 23:59:59.500'), ('1969-12-28 23:59:59.900'), ('1969-12-29 00:00:00.500'), ('1969-12-29 12:00:00.000');

-- The modes that number Sunday above Monday are the ones that expose it: the derived range runs
-- backwards there, so it is empty and discards the whole granule.
SELECT count() FROM t_day_of_week_negative_fraction WHERE toDayOfWeek(dt) = 7;
SELECT count() FROM t_day_of_week_negative_fraction WHERE toDayOfWeek(dt) = 1;
SELECT count() FROM t_day_of_week_negative_fraction WHERE toDayOfWeek(dt, 1) = 6;
SELECT count() FROM t_day_of_week_negative_fraction WHERE toDayOfWeek(dt, 1) = 0;

DROP TABLE t_day_of_week_negative_fraction;
