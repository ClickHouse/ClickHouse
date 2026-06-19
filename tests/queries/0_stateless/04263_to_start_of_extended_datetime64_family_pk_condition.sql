-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET optimize_use_implicit_projections = 0;

-- { echo }

-- The narrow `toStartOf{Day,Hour,Minute,FiveMinutes,TenMinutes,FifteenMinutes}` cast their
-- result to `UInt32`, which wraps for pre-`1970-01-01` and post-`2106-02-07` `DateTime64`
-- inputs (unless the per-session `enable_extended_results_for_datetime_functions` setting
-- is on). The new `*Extended` variants always return `DateTime64` and are honestly
-- monotonic on the full input range, so they can be used safely in `WHERE` filters that
-- span the epoch or the `UInt32`-seconds boundary.

DROP TABLE IF EXISTS t_dt64_cross_epoch;
CREATE TABLE t_dt64_cross_epoch (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dt64_cross_epoch VALUES ('1960-01-15 00:00:00.000'), ('1969-12-31 22:30:00.000'), ('1970-01-15 02:00:00.000');

-- The narrow versions wrap for pre-epoch input; the `*Extended` versions stay signed.
SELECT toStartOfDay(dt)             AS narrow, toStartOfDayExtended(dt)             AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfHour(dt)            AS narrow, toStartOfHourExtended(dt)            AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfMinute(dt)          AS narrow, toStartOfMinuteExtended(dt)          AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfFiveMinutes(dt)     AS narrow, toStartOfFiveMinutesExtended(dt)     AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfTenMinutes(dt)      AS narrow, toStartOfTenMinutesExtended(dt)      AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfFifteenMinutes(dt)  AS narrow, toStartOfFifteenMinutesExtended(dt)  AS extended FROM t_dt64_cross_epoch ORDER BY dt;

-- PK pruning with `*Extended` on a column spanning the epoch: filter matches the post-epoch
-- row, pruning leaves at least the matching granule.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_cross_epoch WHERE toStartOfHourExtended(dt) >= toDateTime64('1969-12-31 22:00:00', 0, 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_cross_epoch WHERE toStartOfHourExtended(dt) >= toDateTime64('1969-12-31 22:00:00', 0, 'UTC') SETTINGS force_primary_key = 1;

-- DateTime64 past 2106-02-07 06:28:16 (the narrow UInt32-seconds wrap): narrow
-- `toStartOfHour` etc. wrap and pruning silently drops matching granules.
-- `*Extended` keeps growing past the UInt32 boundary and stays monotonic.
DROP TABLE IF EXISTS t_dt64_post_2106;
CREATE TABLE t_dt64_post_2106 (dt DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 3;
INSERT INTO t_dt64_post_2106 VALUES (4100000000::DateTime64(0,'UTC')), (4290000000::DateTime64(0,'UTC')), (5680000000::DateTime64(0,'UTC'));

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_2106 WHERE toStartOfHourExtended(dt) >= toDateTime64('2106-02-07 00:00:00', 0, 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_2106 WHERE toStartOfHourExtended(dt) >= toDateTime64('2106-02-07 00:00:00', 0, 'UTC') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_2106 WHERE toStartOfMinuteExtended(dt) >= toDateTime64('2106-02-07 00:00:00', 0, 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_2106 WHERE toStartOfMinuteExtended(dt) >= toDateTime64('2106-02-07 00:00:00', 0, 'UTC') SETTINGS force_primary_key = 1;

-- Happy path: post-epoch table, every variant prunes the expected number of granules.
DROP TABLE IF EXISTS t_dt64_post_epoch;
CREATE TABLE t_dt64_post_epoch (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dt64_post_epoch SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + INTERVAL number HOUR FROM numbers(100);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfDayExtended(dt)            >= toStartOfDayExtended(toDateTime64('2020-01-03 00:00:00.000', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfDayExtended(dt)            >= toStartOfDayExtended(toDateTime64('2020-01-03 00:00:00.000', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfHourExtended(dt)           >= toStartOfHourExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfHourExtended(dt)           >= toStartOfHourExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfMinuteExtended(dt)         >= toStartOfMinuteExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfMinuteExtended(dt)         >= toStartOfMinuteExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfFiveMinutesExtended(dt)    >= toStartOfFiveMinutesExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfFiveMinutesExtended(dt)    >= toStartOfFiveMinutesExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfTenMinutesExtended(dt)     >= toStartOfTenMinutesExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfTenMinutesExtended(dt)     >= toStartOfTenMinutesExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfFifteenMinutesExtended(dt) >= toStartOfFifteenMinutesExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfFifteenMinutesExtended(dt) >= toStartOfFifteenMinutesExtended(toDateTime64('2020-01-03 12:00:00.000', 3, 'UTC')) SETTINGS force_primary_key = 1;

DROP TABLE t_dt64_cross_epoch;
DROP TABLE t_dt64_post_2106;
DROP TABLE t_dt64_post_epoch;
