-- A string literal inside a function in a mutation predicate is read in the session timezone, like in a `SELECT`.

SET session_timezone = 'America/Denver'; -- far from a typical server timezone
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_mut_fn_tz;
CREATE TABLE t_mut_fn_tz (id UInt32, time DateTime, time64 DateTime64(3), v String DEFAULT '') ENGINE = MergeTree ORDER BY id;

-- Pair `k` is two rows with the same wall-clock string, read in UTC (id `2k - 1`) and in `America/Denver` (id `2k`).
-- The mutation of case `k` has to affect id `2k`, the row the same `SELECT` finds in the session timezone.
INSERT INTO t_mut_fn_tz (id, time, time64)
WITH concat('2000-01-0', toString(1 + intDiv(number, 7)), ' 0', toString(1 + number % 7), ':00:00') AS wall_clock
SELECT 2 * (number + 1) - 1, toDateTime(wall_clock, 'UTC'), toDateTime64(wall_clock, 3, 'UTC') FROM numbers(11);
INSERT INTO t_mut_fn_tz (id, time, time64)
WITH concat('2000-01-0', toString(1 + intDiv(number, 7)), ' 0', toString(1 + number % 7), ':00:00') AS wall_clock
SELECT 2 * (number + 1), toDateTime(wall_clock, 'America/Denver'), toDateTime64(wall_clock, 3, 'America/Denver') FROM numbers(11);

ALTER TABLE t_mut_fn_tz DELETE WHERE time = toDateTime('2000-01-01 01:00:00');
ALTER TABLE t_mut_fn_tz DELETE WHERE time64 = toDateTime64('2000-01-01 02:00:00', 3);
ALTER TABLE t_mut_fn_tz DELETE WHERE time = CAST('2000-01-01 03:00:00' AS DateTime);
ALTER TABLE t_mut_fn_tz DELETE WHERE time64 = '2000-01-01 04:00:00'::DateTime64(3);
ALTER TABLE t_mut_fn_tz DELETE WHERE time = parseDateTimeBestEffort('2000-01-01 05:00:00');
ALTER TABLE t_mut_fn_tz DELETE WHERE time = parseDateTime('2000-01-01 06:00:00', '%Y-%m-%d %H:%i:%s');
ALTER TABLE t_mut_fn_tz UPDATE v = 'updated' WHERE time = toDateTime('2000-01-01 07:00:00');
DELETE FROM t_mut_fn_tz WHERE time = toDateTime('2000-01-02 01:00:00');
ALTER TABLE t_mut_fn_tz DELETE WHERE time IN (toDateTime('2000-01-02 02:00:00'));
ALTER TABLE t_mut_fn_tz DELETE WHERE toDateTime('2000-01-02 03:00:00') = time;
-- An explicit timezone is kept as it is: the UTC row (id 21) goes, not the Denver one.
ALTER TABLE t_mut_fn_tz DELETE WHERE time = toDateTime('2000-01-02 04:00:00', 'UTC');

SELECT id, v FROM t_mut_fn_tz ORDER BY id;

DROP TABLE t_mut_fn_tz;
