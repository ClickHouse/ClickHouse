-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET optimize_use_implicit_projections = 0;

-- { echo }

-- The narrow `toStartOf{Month,Quarter,Year,Week}` and `toMonday`/`toLastDayOf{Month,Week}`
-- cast their result to `UInt16`, which wraps for pre-`1970-01-01` and post-`2149-06-06`
-- `Date32`/`DateTime64` inputs (unless the per-session `enable_extended_results_for_datetime_functions`
-- setting is on). The new `*Extended` variants always return `Date32` and are honestly
-- monotonic on the full input range, so they can be used safely in `WHERE` filters that
-- span the epoch or the `Date` upper bound.

DROP TABLE IF EXISTS t_dt64_cross_epoch;
CREATE TABLE t_dt64_cross_epoch (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dt64_cross_epoch VALUES ('1960-01-15 00:00:00.000'), ('1969-12-31 22:00:00.000'), ('1970-01-15 02:00:00.000');

-- The narrow versions wrap for pre-epoch input; the `*Extended` versions stay signed.
SELECT toMonday(dt)          AS narrow, toMondayExtended(dt)          AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfMonth(dt)    AS narrow, toStartOfMonthExtended(dt)    AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toLastDayOfMonth(dt)  AS narrow, toLastDayOfMonthExtended(dt)  AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfQuarter(dt)  AS narrow, toStartOfQuarterExtended(dt)  AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfYear(dt)     AS narrow, toStartOfYearExtended(dt)     AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toStartOfWeek(dt)     AS narrow, toStartOfWeekExtended(dt)     AS extended FROM t_dt64_cross_epoch ORDER BY dt;
SELECT toLastDayOfWeek(dt)   AS narrow, toLastDayOfWeekExtended(dt)   AS extended FROM t_dt64_cross_epoch ORDER BY dt;

-- PK pruning with `*Extended` on a column spanning the epoch: filter matches the post-epoch
-- row, pruning leaves at least the matching granule.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_cross_epoch WHERE toStartOfMonthExtended(dt) >= toDate32('1969-01-01')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_cross_epoch WHERE toStartOfMonthExtended(dt) >= toDate32('1969-01-01') SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_cross_epoch WHERE toStartOfYearExtended(dt) >= toDate32('1969-01-01')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_cross_epoch WHERE toStartOfYearExtended(dt) >= toDate32('1969-01-01') SETTINGS force_primary_key = 1;

-- Date32 past 2149-06-06 (the narrow `Date` upper bound): narrow `toStartOfMonth` etc.
-- wrap UInt16 day nums past 65535 and pruning silently drops matching granules.
-- `*Extended` keeps growing and stays monotonic.
DROP TABLE IF EXISTS t_d32_post_2149;
CREATE TABLE t_d32_post_2149 (d Date32) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 3;
INSERT INTO t_d32_post_2149 VALUES ('2150-01-15'), ('2200-06-15'), ('2299-12-31');

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_d32_post_2149 WHERE toStartOfYearExtended(d) >= toDate32('2150-01-01') SETTINGS optimize_use_implicit_projections = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_d32_post_2149 WHERE toStartOfYearExtended(d) >= toDate32('2150-01-01') SETTINGS optimize_use_implicit_projections = 0 SETTINGS force_primary_key = 1;

-- Happy path: post-epoch table, every variant prunes the expected number of granules.
DROP TABLE IF EXISTS t_dt64_post_epoch;
CREATE TABLE t_dt64_post_epoch (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dt64_post_epoch SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + INTERVAL number MONTH FROM numbers(100);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toMondayExtended(dt)         >= toMondayExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toMondayExtended(dt)         >= toMondayExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfMonthExtended(dt)   >= toStartOfMonthExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfMonthExtended(dt)   >= toStartOfMonthExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toLastDayOfMonthExtended(dt) >= toLastDayOfMonthExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toLastDayOfMonthExtended(dt) >= toLastDayOfMonthExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfQuarterExtended(dt) >= toStartOfQuarterExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfQuarterExtended(dt) >= toStartOfQuarterExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfYearExtended(dt)    >= toStartOfYearExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfYearExtended(dt)    >= toStartOfYearExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toStartOfWeekExtended(dt)    >= toStartOfWeekExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toStartOfWeekExtended(dt)    >= toStartOfWeekExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dt64_post_epoch WHERE toLastDayOfWeekExtended(dt)  >= toLastDayOfWeekExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';
SELECT count() FROM t_dt64_post_epoch WHERE toLastDayOfWeekExtended(dt)  >= toLastDayOfWeekExtended(toDateTime64('2024-05-15 12:34:56.789', 3, 'UTC')) SETTINGS force_primary_key = 1;

DROP TABLE t_dt64_cross_epoch;
DROP TABLE t_d32_post_2149;
DROP TABLE t_dt64_post_epoch;
