SET session_timezone = 'UTC';

-- A week function called with an explicit time zone takes three arguments, and the monotonic function
-- chain of `KeyCondition` carries at most one constant argument - `isKeyPossiblyWrappedByMonotonicFunctions`
-- curries binary functions only - so such a call is not analysed for the primary key at all. Were its
-- `week_mode` and `timezone` constants dropped instead, the bounds of the key range would be mapped by
-- the default-mode, session-time-zone function, and the derived range would discard granules that hold
-- matching rows.

DROP TABLE IF EXISTS t_week_explicit_tz;

CREATE TABLE t_week_explicit_tz (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0;

-- 2026-08-09 is a Sunday in UTC and every row below stays inside that one UTC day, while Tokyo is nine
-- hours ahead, so from 15:00 UTC on it is already Monday there.

INSERT INTO t_week_explicit_tz SELECT toDateTime('2026-08-09 13:00:00', 'UTC') + (1800 * number) FROM numbers(8);

SELECT countIf(toDayOfWeek(dt, 0, 'Asia/Tokyo') = 1), count() FROM t_week_explicit_tz WHERE toDayOfWeek(dt, 0, 'Asia/Tokyo') = 1;
SELECT countIf(toDayOfWeek(dt, 0, 'Asia/Tokyo') = 7), count() FROM t_week_explicit_tz WHERE toDayOfWeek(dt, 0, 'Asia/Tokyo') = 7;

-- `toStartOfWeek`, `toLastDayOfWeek` and `toYearWeek` claim monotonicity unconditionally, so they are
-- the sharpest case: with the explicit time zone dropped, the range would be mapped to the UTC week of
-- 2026-08-03 and none of these needles would fall inside it.

SELECT countIf(toStartOfWeek(dt, 1, 'Asia/Tokyo') = toDate('2026-08-10')), count() FROM t_week_explicit_tz WHERE toStartOfWeek(dt, 1, 'Asia/Tokyo') = toDate('2026-08-10');
SELECT countIf(toLastDayOfWeek(dt, 1, 'Asia/Tokyo') = toDate('2026-08-16')), count() FROM t_week_explicit_tz WHERE toLastDayOfWeek(dt, 1, 'Asia/Tokyo') = toDate('2026-08-16');
SELECT countIf(toYearWeek(dt, 1, 'Asia/Tokyo') = 202633), count() FROM t_week_explicit_tz WHERE toYearWeek(dt, 1, 'Asia/Tokyo') = 202633;

DROP TABLE t_week_explicit_tz;
