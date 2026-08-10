SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04841;
CREATE TABLE logs_04841
(
    `_time` DateTime,
    `_msg` String,
    `user_id` String,
    `bytes` UInt64
) ENGINE = MergeTree ORDER BY _time;

-- u1 has 3 rows inside the outer one-day window, u2 has 3 rows outside it.
INSERT INTO logs_04841 VALUES
    ('2024-01-01 00:00:00', 'a', 'u1', 10),
    ('2024-01-01 01:00:00', 'b', 'u1', 20),
    ('2024-01-01 02:00:00', 'c', 'u1', 30),
    ('2024-01-02 00:00:00', 'd', 'u2', 40),
    ('2024-01-02 01:00:00', 'e', 'u2', 50),
    ('2024-01-02 02:00:00', 'f', 'u2', 60);

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04841';
SET dialect = 'logsql';

-- The `rate()` denominator of a subquery must not be inherited from the outer query:
-- the subquery does not get the outer `_time` predicate in its `WHERE`, so it scans the
-- whole table and its `rate()` has no window of its own, i.e. it degrades to `count()`.
-- Both users have 3 rows, so both pass `r >= 1` and the outer window selects u1's 3 rows.
-- If the outer one-day window leaked into the subquery, every rate would be 3/86400,
-- no user would pass and the count would be 0.
_time:[2024-01-01Z, 2024-01-02Z) user_id:in(* | stats by (user_id) rate() as r | where r:>=1 | fields user_id) | count();

-- The same for `rate_sum()`.
_time:[2024-01-01Z, 2024-01-02Z) user_id:in(* | stats by (user_id) rate_sum(bytes) as r | where r:>=10 | fields user_id) | count();

-- A subquery with a window of its own uses only that window, not its intersection with the
-- outer one: two days = 172800 seconds, so both users have a rate of 3/172800 < 2e-5 and
-- nothing matches. Intersecting with the outer one-day window would give 3/86400 > 2e-5.
_time:[2024-01-01Z, 2024-01-02Z) user_id:in(_time:[2024-01-01Z, 2024-01-03Z) * | stats by (user_id) rate() as r | where r:>2e-5 | fields user_id) | count();

-- The outer `_time` range is still the denominator of the outer `rate()` itself: 3/86400.
_time:[2024-01-01Z, 2024-01-02Z) * | stats rate() as r | fields r;

-- An outer `_time` bucket of a stats pipe does not leak into a subquery of a later pipe
-- either: the inner `rate()` has no window, so it equals `count()` and both users pass.
-- Every `stats` pipe starts by resetting the bucket, and a subquery clears it on entry.
* | stats by (_time:1h, user_id) count() as c | filter user_id:in(* | stats by (user_id) rate() as r | where r:>=1 | fields user_id) | stats count() as buckets | fields buckets;

SET dialect = 'clickhouse';
DROP TABLE logs_04841;
