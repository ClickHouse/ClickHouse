-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET optimize_use_implicit_projections = 0;

-- { echo }

-- `toRelative{Week,Day,Hour,Minute,Second}Num` cast their result to `UInt16`/`UInt32`,
-- which wraps for pre-epoch `DateTime64` and (for `SecondNum`) for timestamps past
-- `2106-02-07 06:28:16`. The new `*NumExtended` variants return signed `Int64` and avoid the
-- wrap, so they are honestly monotonic on the full input range and can be used safely
-- in `WHERE` filters that span the epoch or the `UInt32` second boundary.

DROP TABLE IF EXISTS t_dt64;
CREATE TABLE t_dt64 (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dt64 VALUES ('1960-01-01 00:00:00.000'), ('1969-12-31 22:00:00.000'), ('1970-01-01 02:00:00.000');

-- The narrow versions wrap for pre-epoch input; the `*NumExtended` versions stay signed.
SELECT toRelativeWeekNum(dt)     AS week,   toRelativeWeekNumExtended(dt)   AS week64   FROM t_dt64 ORDER BY dt;
SELECT toRelativeDayNum(dt)      AS day,    toRelativeDayNumExtended(dt)    AS day64    FROM t_dt64 ORDER BY dt;
SELECT toRelativeHourNum(dt)     AS hour,   toRelativeHourNumExtended(dt)   AS hour64   FROM t_dt64 ORDER BY dt;
SELECT toRelativeMinuteNum(dt)   AS minute, toRelativeMinuteNumExtended(dt) AS minute64 FROM t_dt64 ORDER BY dt;
SELECT toRelativeSecondNum(dt)   AS second, toRelativeSecondNumExtended(dt) AS second64 FROM t_dt64 ORDER BY dt;

-- PK pruning with `*NumExtended` on a column spanning the epoch. The filters are chosen so
-- they match at least one row; what matters is that PK pruning keeps the granules containing
-- the matching values — see the EXPLAIN.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64 WHERE toRelativeHourNumExtended(dt) BETWEEN 0 AND 100) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64 WHERE toRelativeHourNumExtended(dt) BETWEEN 0 AND 100 SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64 WHERE toRelativeSecondNumExtended(dt) BETWEEN -10000 AND 10000) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64 WHERE toRelativeSecondNumExtended(dt) BETWEEN -10000 AND 10000 SETTINGS force_primary_key = 1;

-- Post-`UInt32`-second boundary (after 2106-02-07): the narrow `toRelativeSecondNum`
-- wraps and pruning silently drops matching granules. `*NumExtended` keeps growing past
-- `2^32` and stays monotonic.

DROP TABLE IF EXISTS t_dt64_post_2106;
CREATE TABLE t_dt64_post_2106 (dt DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 3;
INSERT INTO t_dt64_post_2106 VALUES (4100000000::DateTime64(0,'UTC')), (4290000000::DateTime64(0,'UTC')), (5680000000::DateTime64(0,'UTC'));

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_2106 WHERE toRelativeSecondNumExtended(dt) BETWEEN 4250000000 AND 4295000000) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_2106 WHERE toRelativeSecondNumExtended(dt) BETWEEN 4250000000 AND 4295000000 SETTINGS force_primary_key = 1;

-- Happy path: post-epoch table, every variant prunes the expected number of granules.
DROP TABLE IF EXISTS t_dt64_post_epoch;
CREATE TABLE t_dt64_post_epoch (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dt64_post_epoch SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + INTERVAL number MONTH FROM numbers(100);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toRelativeWeekNumExtended(dt)   >= toRelativeWeekNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toRelativeWeekNumExtended(dt)   >= toRelativeWeekNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toRelativeDayNumExtended(dt)    >= toRelativeDayNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toRelativeDayNumExtended(dt)    >= toRelativeDayNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toRelativeHourNumExtended(dt)   >= toRelativeHourNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toRelativeHourNumExtended(dt)   >= toRelativeHourNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toRelativeMinuteNumExtended(dt) >= toRelativeMinuteNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toRelativeMinuteNumExtended(dt) >= toRelativeMinuteNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toRelativeSecondNumExtended(dt) >= toRelativeSecondNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toRelativeSecondNumExtended(dt) >= toRelativeSecondNumExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

DROP TABLE t_dt64;
DROP TABLE t_dt64_post_2106;
DROP TABLE t_dt64_post_epoch;
