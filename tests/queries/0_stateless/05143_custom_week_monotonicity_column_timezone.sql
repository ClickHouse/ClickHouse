SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_week_tz;

-- The week functions are executed in the time zone of their argument, so their monotonicity must be
-- decided in that time zone too: a key range inside one week of the session time zone can straddle a
-- week boundary of the column's time zone, and primary-key pruning then dropped matching rows.

CREATE TABLE t_week_tz (dt DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_week_tz SELECT toDateTime('2026-08-09 10:00:00', 'UTC') + (3600 * number) FROM numbers(11);

SELECT countIf(toDayOfWeek(dt) = 7), count() FROM t_week_tz WHERE toDayOfWeek(dt) = 7;
SELECT countIf(toDayOfWeek(dt) = 1), count() FROM t_week_tz WHERE toDayOfWeek(dt) = 1;
SELECT countIf(toWeek(dt) = 32), count() FROM t_week_tz WHERE toWeek(dt) = 32;
SELECT countIf(toStartOfWeek(dt) = toDate('2026-08-09')), count() FROM t_week_tz WHERE toStartOfWeek(dt) = toDate('2026-08-09');

DROP TABLE t_week_tz;

-- The same data in a column whose time zone is the session one is unaffected.

CREATE TABLE t_week_tz (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_week_tz SELECT toDateTime('2026-08-09 10:00:00', 'UTC') + (3600 * number) FROM numbers(11);

SELECT countIf(toDayOfWeek(dt) = 7), count() FROM t_week_tz WHERE toDayOfWeek(dt) = 7;
SELECT countIf(toWeek(dt) = 32), count() FROM t_week_tz WHERE toWeek(dt) = 32;

DROP TABLE t_week_tz;

-- `toDayOfWeek` numbers Sunday lowest in the Sunday-first modes, so a range inside one Monday-week is
-- not monotonic there and must not be used to discard the part that holds a matching row.

DROP TABLE IF EXISTS t_day_of_week_mode;

CREATE TABLE t_day_of_week_mode (d Date) ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_day_of_week_mode SELECT toDate('2026-08-03') + number FROM numbers(7);

SELECT countIf(toDayOfWeek(d, 3) = 1), count() FROM t_day_of_week_mode WHERE toDayOfWeek(d, 3) = 1;
SELECT countIf(toDayOfWeek(d, 2) = 0), count() FROM t_day_of_week_mode WHERE toDayOfWeek(d, 2) = 0;
SELECT countIf(toDayOfWeek(d) = 7), count() FROM t_day_of_week_mode WHERE toDayOfWeek(d) = 7;
SELECT countIf(toDayOfWeek(d, 1) = 0), count() FROM t_day_of_week_mode WHERE toDayOfWeek(d, 1) = 0;

DROP TABLE t_day_of_week_mode;
