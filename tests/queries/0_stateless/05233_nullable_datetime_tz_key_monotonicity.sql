-- https://github.com/ClickHouse/ClickHouse/issues/121179
-- Date-part predicates on a Nullable(DateTime('tz')) ORDER BY key used to prune granules with the
-- session time zone instead of the time zone attached to the key type, silently dropping rows.

SET session_timezone = 'UTC';
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_dt_nullable;
DROP TABLE IF EXISTS t_dt_lc_nullable;
DROP TABLE IF EXISTS t_dt64_nullable;
DROP TABLE IF EXISTS t_dt_plain;

CREATE TABLE t_dt_nullable (dt Nullable(DateTime('America/New_York')))
ENGINE = MergeTree ORDER BY dt SETTINGS allow_nullable_key = 1;

CREATE TABLE t_dt_lc_nullable (dt LowCardinality(Nullable(DateTime('America/New_York'))))
ENGINE = MergeTree ORDER BY dt SETTINGS allow_nullable_key = 1;

CREATE TABLE t_dt64_nullable (dt Nullable(DateTime64(3, 'America/New_York')))
ENGINE = MergeTree ORDER BY dt SETTINGS allow_nullable_key = 1;

-- non-nullable control: this path already worked, a regression here must be visible too
CREATE TABLE t_dt_plain (dt DateTime('America/New_York'))
ENGINE = MergeTree ORDER BY dt;

-- All three timestamps fall on the same day (and month) in UTC, but straddle midnight in New York,
-- so the factor transform evaluated in UTC wrongly declares the range monotonic.
INSERT INTO t_dt_nullable VALUES ('2024-01-31 23:00:00'), ('2024-02-01 00:30:00'), ('2024-02-01 01:00:00');
INSERT INTO t_dt_lc_nullable VALUES ('2024-01-31 23:00:00'), ('2024-02-01 00:30:00'), ('2024-02-01 01:00:00');
INSERT INTO t_dt64_nullable VALUES ('2024-01-31 23:00:00.000'), ('2024-02-01 00:30:00.000'), ('2024-02-01 01:00:00.000');
INSERT INTO t_dt_plain VALUES ('2024-01-31 23:00:00'), ('2024-02-01 00:30:00'), ('2024-02-01 01:00:00');

-- Each row is "<pruned count>\t<countIf over the whole table>"; the two must always agree.
SELECT (SELECT count() FROM t_dt_nullable WHERE toHour(dt) = 23), (SELECT countIf(toHour(dt) = 23) FROM t_dt_nullable);
SELECT (SELECT count() FROM t_dt_lc_nullable WHERE toHour(dt) = 23), (SELECT countIf(toHour(dt) = 23) FROM t_dt_lc_nullable);
SELECT (SELECT count() FROM t_dt64_nullable WHERE toHour(dt) = 23), (SELECT countIf(toHour(dt) = 23) FROM t_dt64_nullable);
SELECT (SELECT count() FROM t_dt_plain WHERE toHour(dt) = 23), (SELECT countIf(toHour(dt) = 23) FROM t_dt_plain);

SELECT (SELECT count() FROM t_dt_nullable WHERE toDayOfMonth(dt) = 31), (SELECT countIf(toDayOfMonth(dt) = 31) FROM t_dt_nullable);
SELECT (SELECT count() FROM t_dt_lc_nullable WHERE toDayOfMonth(dt) = 31), (SELECT countIf(toDayOfMonth(dt) = 31) FROM t_dt_lc_nullable);
SELECT (SELECT count() FROM t_dt64_nullable WHERE toDayOfMonth(dt) = 31), (SELECT countIf(toDayOfMonth(dt) = 31) FROM t_dt64_nullable);
SELECT (SELECT count() FROM t_dt_plain WHERE toDayOfMonth(dt) = 31), (SELECT countIf(toDayOfMonth(dt) = 31) FROM t_dt_plain);

-- `toDayOfWeek` goes through `IFunctionCustomWeek::getMonotonicityForRange` instead, whose factor
-- transform is `toMonday` -- fine enough to misprune. These timestamps sit in one Monday-based week
-- in UTC but in two different weeks in New York.
INSERT INTO t_dt_nullable VALUES ('2024-01-07 23:30:00'), ('2024-01-08 00:30:00');
INSERT INTO t_dt_lc_nullable VALUES ('2024-01-07 23:30:00'), ('2024-01-08 00:30:00');
INSERT INTO t_dt64_nullable VALUES ('2024-01-07 23:30:00.000'), ('2024-01-08 00:30:00.000');
INSERT INTO t_dt_plain VALUES ('2024-01-07 23:30:00'), ('2024-01-08 00:30:00');

SELECT (SELECT count() FROM t_dt_nullable WHERE toDayOfWeek(dt) = 7), (SELECT countIf(toDayOfWeek(dt) = 7) FROM t_dt_nullable);
SELECT (SELECT count() FROM t_dt_lc_nullable WHERE toDayOfWeek(dt) = 7), (SELECT countIf(toDayOfWeek(dt) = 7) FROM t_dt_lc_nullable);
SELECT (SELECT count() FROM t_dt64_nullable WHERE toDayOfWeek(dt) = 7), (SELECT countIf(toDayOfWeek(dt) = 7) FROM t_dt64_nullable);
SELECT (SELECT count() FROM t_dt_plain WHERE toDayOfWeek(dt) = 7), (SELECT countIf(toDayOfWeek(dt) = 7) FROM t_dt_plain);

SELECT (SELECT count() FROM t_dt_nullable WHERE toDayOfWeek(dt) = 1), (SELECT countIf(toDayOfWeek(dt) = 1) FROM t_dt_nullable);
SELECT (SELECT count() FROM t_dt_lc_nullable WHERE toDayOfWeek(dt) = 1), (SELECT countIf(toDayOfWeek(dt) = 1) FROM t_dt_lc_nullable);
SELECT (SELECT count() FROM t_dt64_nullable WHERE toDayOfWeek(dt) = 1), (SELECT countIf(toDayOfWeek(dt) = 1) FROM t_dt64_nullable);
SELECT (SELECT count() FROM t_dt_plain WHERE toDayOfWeek(dt) = 1), (SELECT countIf(toDayOfWeek(dt) = 1) FROM t_dt_plain);

-- Pruning must still happen on a Nullable key. These timestamps are midday in New York, so UTC and
-- New York agree on the day: the time zone the pruner picks cannot change the answer, which makes
-- this arm a pure guard against a future "fix" that simply reports wrapped types as non-monotonic.
DROP TABLE IF EXISTS t_dt_prune;
CREATE TABLE t_dt_prune (dt Nullable(DateTime('America/New_York')))
ENGINE = MergeTree ORDER BY dt SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO t_dt_prune VALUES ('2024-02-14 12:00:00'), ('2024-02-14 13:00:00'), ('2024-02-15 12:00:00'), ('2024-02-15 13:00:00'), ('2024-02-16 12:00:00'), ('2024-02-16 13:00:00');

SELECT (SELECT count() FROM t_dt_prune WHERE toDayOfMonth(dt) = 15), (SELECT countIf(toDayOfMonth(dt) = 15) FROM t_dt_prune);
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_dt_prune WHERE toDayOfMonth(dt) = 15
)
WHERE trim(explain) ilike 'granules: %';

DROP TABLE t_dt_prune;

DROP TABLE t_dt_nullable;
DROP TABLE t_dt_lc_nullable;
DROP TABLE t_dt64_nullable;
DROP TABLE t_dt_plain;
