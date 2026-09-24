-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';

-- Test MergeTreeSetIndex Roaring Bitmap fast path for single integer key mark pruning.
DROP TABLE IF EXISTS t_roaring_index;
DROP TABLE IF EXISTS t_roaring_compound;
DROP TABLE IF EXISTS t_roaring_index_u32;
DROP TABLE IF EXISTS t_roaring_transformed;

CREATE TABLE t_roaring_index (id UInt64, value Float64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_roaring_index SELECT number, number * 1.5 FROM numbers(100);

SELECT '--- Basic IN subset ---';
SELECT id FROM t_roaring_index WHERE id IN (SELECT number FROM numbers(100) WHERE number % 20 == 0) ORDER BY id;

SELECT '--- Point lookup via IN ---';
SELECT id, value FROM t_roaring_index WHERE id IN (SELECT 42::UInt64);

SELECT '--- Narrow range in set ---';
SELECT count() FROM t_roaring_index WHERE id IN (SELECT number FROM numbers(100) WHERE number BETWEEN 10 AND 12);

SELECT '--- Single element NOT IN ---';
SELECT id FROM t_roaring_index WHERE id NOT IN (SELECT number FROM numbers(100) WHERE number != 50) ORDER BY id;

SELECT '--- Transformed key: intDiv(id, 10) IN (2, 4) ---';
SELECT id FROM t_roaring_index WHERE intDiv(id, 10) IN (2, 4) ORDER BY id;

SELECT '--- UInt32 key column ---';
CREATE TABLE t_roaring_index_u32 (id UInt32, value Float64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_roaring_index_u32 SELECT number, number * 2.0 FROM numbers(50);

SELECT id FROM t_roaring_index_u32 WHERE id IN (SELECT toUInt32(number * 10) FROM numbers(5)) ORDER BY id;

SELECT '--- Transformed key: toWeek(dt) and toMonth(dt) IN (...) ---';
CREATE TABLE t_roaring_transformed (dt DateTime, value Float64) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8;
INSERT INTO t_roaring_transformed SELECT toDateTime('2026-01-01 00:00:00') + interval (number * 3) day, number * 1.0 FROM numbers(50);

SELECT count() FROM t_roaring_transformed WHERE toWeek(dt) IN (1, 3, 5);
SELECT toDate(dt), toWeek(dt) FROM t_roaring_transformed WHERE toWeek(dt) IN (1, 3) ORDER BY dt;
SELECT count() FROM t_roaring_transformed WHERE toMonth(dt) IN (1, 2);

SELECT '--- Transformed key: toDayOfMonth with timezone and Nullable ---';
CREATE TABLE t_roaring_tz (dt Nullable(DateTime('America/New_York')), value Float64) ENGINE = MergeTree ORDER BY dt DESC SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_roaring_tz SELECT toDateTime('2026-01-01 00:00:00', 'America/New_York') + interval (number * 3) day, number * 1.0 FROM numbers(50);
SELECT count() FROM t_roaring_tz WHERE toDayOfMonth(dt) IN (1, 3);
DROP TABLE t_roaring_tz;

SELECT '--- Transformed key: toDayOfMonth on Nullable(DateTime) whose timezone differs from the session ---';
-- The monotonicity check must use the column timezone, not the session one, also through `Nullable`.
-- The only granule spans 2026-11-30 22:00 .. 2026-12-31 12:00 in New York, which is 2026-12-01 03:00 ..
-- 2026-12-31 17:00 in UTC. With the session timezone the range looks like a single month, so
-- `toDayOfMonth` would be treated as monotonic, map to [30, 31], and the granule would be pruned,
-- losing the row for day 5.
SET session_timezone = 'UTC';
CREATE TABLE t_roaring_tz_nullable (dt Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_roaring_tz_nullable VALUES (toDateTime('2026-11-30 22:00:00', 'America/New_York')), (toDateTime('2026-12-05 12:00:00', 'America/New_York')), (toDateTime('2026-12-31 12:00:00', 'America/New_York'));
SELECT dt FROM t_roaring_tz_nullable WHERE toDayOfMonth(dt) IN (5);
DROP TABLE t_roaring_tz_nullable;
SET session_timezone = DEFAULT;

SELECT '--- Transformed key: custom-week functions on DateTime whose timezone differs from the session ---';
-- Same as above for `IFunctionCustomWeek`. Each part is one granule. The second one spans Sunday 20:00 ..
-- Monday 05:00 in Tokyo, which is Sunday 11:00 .. 20:00 in UTC. With the session timezone both ends
-- fall into one Monday-based week, so `toDayOfWeek` would be treated as monotonic, map to [7, 1], and
-- the granule would be pruned, losing the Sunday 20:00 row. For `IN` the fast path above treats the
-- inverted range conservatively, so only the comparison shows it. `toStartOfWeek`, `toYearWeek` and
-- `toLastDayOfWeek` are always monotonic, so the first part, whose ends are in different local weeks,
-- is kept for them either way.
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS t_roaring_tz_week;
CREATE TABLE t_roaring_tz_week (ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_roaring_tz_week;
INSERT INTO t_roaring_tz_week VALUES ('2026-06-06 20:00:00'), ('2026-06-07 05:00:00');
INSERT INTO t_roaring_tz_week VALUES ('2026-06-07 20:00:00'), ('2026-06-08 05:00:00');
SELECT 'toDayOfWeek', ts FROM t_roaring_tz_week WHERE toDayOfWeek(ts) = 7 ORDER BY ts;
SELECT 'toDayOfWeek IN', ts FROM t_roaring_tz_week WHERE toDayOfWeek(ts) IN (7) ORDER BY ts;
SELECT 'toStartOfWeek', ts FROM t_roaring_tz_week WHERE toStartOfWeek(ts) IN ('2026-06-07') ORDER BY ts;
SELECT 'toYearWeek', ts FROM t_roaring_tz_week WHERE toYearWeek(ts) IN (202622) ORDER BY ts;
SELECT 'toLastDayOfWeek', ts FROM t_roaring_tz_week WHERE toLastDayOfWeek(ts) IN ('2026-06-06') ORDER BY ts;
DROP TABLE t_roaring_tz_week;
SET session_timezone = DEFAULT;

SELECT '--- Transformed key: date functions with constant mode or timezone arguments ---';
-- A constant second argument is kept in the monotonic chain, so the week mode and the explicit timezone
-- take part in the monotonicity check. A call with three arguments is not used for the primary key
-- at all (`Condition: true`). Were the extra arguments dropped, the bare function would work in UTC:
-- the first part below (Sunday 20:00 .. Monday 05:00 in Tokyo) would map to [7, 7] and the second part
-- (2026-11-30 22:00 .. 2026-12-01 06:00 in Tokyo) to [30, 30], and both would be pruned.
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS t_roaring_tz_args;
CREATE TABLE t_roaring_tz_args (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 8;
SYSTEM STOP MERGES t_roaring_tz_args;
INSERT INTO t_roaring_tz_args VALUES ('2026-06-07 11:00:00'), ('2026-06-07 20:00:00');
INSERT INTO t_roaring_tz_args VALUES ('2026-11-30 13:00:00'), ('2026-11-30 21:00:00');
SELECT 'toDayOfWeek(ts, 0, tz) = 1', ts FROM t_roaring_tz_args WHERE toDayOfWeek(ts, 0, 'Asia/Tokyo') = 1 ORDER BY ts;
SELECT 'toDayOfWeek(ts, 0, tz) IN (1)', ts FROM t_roaring_tz_args WHERE toDayOfWeek(ts, 0, 'Asia/Tokyo') IN (1) ORDER BY ts;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_roaring_tz_args WHERE toDayOfWeek(ts, 0, 'Asia/Tokyo') IN (1)) WHERE explain LIKE '%Condition%';
SELECT 'toDayOfMonth(ts, tz) = 1', ts FROM t_roaring_tz_args WHERE toDayOfMonth(ts, 'Asia/Tokyo') = 1 ORDER BY ts;
SELECT 'toDayOfMonth(ts, tz) IN (1)', ts FROM t_roaring_tz_args WHERE toDayOfMonth(ts, 'Asia/Tokyo') IN (1) ORDER BY ts;
DROP TABLE t_roaring_tz_args;
-- The week mode argument alone keeps the column timezone: Sunday 20:00 .. Monday 05:00 in Tokyo is one
-- UTC week, and in mode 1 the local values are 6 and 0.
DROP TABLE IF EXISTS t_roaring_tz_week_mode;
CREATE TABLE t_roaring_tz_week_mode (ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 8;
INSERT INTO t_roaring_tz_week_mode VALUES ('2026-06-07 20:00:00'), ('2026-06-08 05:00:00');
SELECT 'toDayOfWeek(ts, 1) = 6', ts FROM t_roaring_tz_week_mode WHERE toDayOfWeek(ts, 1) = 6 ORDER BY ts;
SELECT 'toDayOfWeek(ts, 1) IN (0)', ts FROM t_roaring_tz_week_mode WHERE toDayOfWeek(ts, 1) IN (0) ORDER BY ts;
DROP TABLE t_roaring_tz_week_mode;
SET session_timezone = DEFAULT;

SELECT '--- UInt64 ids on both sides of the 32-bit bucket boundary ---';
-- `Roaring64Map` keeps one 32-bit bitmap per high word, so everything above only exercises bucket 0.
-- These ids sit in three different buckets and the ranges below cross the 2^32 seam.
DROP TABLE IF EXISTS t_roaring_wide;
CREATE TABLE t_roaring_wide (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_roaring_wide VALUES (5), (4294967290), (4294967296), (4294967300), (8589934592), (8589934596);

-- one member per bucket
SELECT id FROM t_roaring_wide WHERE id IN (5, 4294967296, 8589934592) ORDER BY id;
-- a set that straddles the seam
SELECT id FROM t_roaring_wide WHERE id IN (4294967290, 4294967300) ORDER BY id;
-- a member that is absent, on the far side of the seam
SELECT count() FROM t_roaring_wide WHERE id IN (4294967295, 8589934595);

SELECT '--- Compound key: set column is not the leading key column ---';
-- The sparse primary-key decomposition can hand the set index an inverted range for a
-- non-leading key column. An inverted range is empty, so the fast path has to prune the
-- mark exactly like the general path, instead of falling back to "may be true".
CREATE TABLE t_roaring_compound (team_id UInt64, k UInt8, s String)
ENGINE = MergeTree ORDER BY (team_id, k, s) SETTINGS index_granularity = 8192;
INSERT INTO t_roaring_compound SELECT 1, number % 5, toString(number) FROM numbers(200000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_roaring_compound WHERE team_id = 1 AND has([0, 3], k)) WHERE explain LIKE '%Granules%';
SELECT count() FROM t_roaring_compound WHERE team_id = 1 AND has([0, 3], k) SETTINGS force_primary_key = 1;

DROP TABLE t_roaring_index;
DROP TABLE t_roaring_index_u32;
DROP TABLE t_roaring_transformed;
DROP TABLE t_roaring_compound;
