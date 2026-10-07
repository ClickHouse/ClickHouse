-- toRelativeDayNum, toYearNumSinceEpoch and toMonthNumSinceEpoch are always monotonic for the primary index, so
-- for a DateTime west of UTC the local day 1969-12-31 must give 0 (as for DateTime64), not wrap to 65535.

SELECT toRelativeDayNum(toDateTime(0, 'America/New_York')), toYearNumSinceEpoch(toDateTime(0, 'America/New_York')), toMonthNumSinceEpoch(toDateTime(0, 'America/New_York'));
SELECT dateDiff('day', toDateTime(0, 'America/New_York'), toDateTime(86400, 'America/New_York')),
       dateDiff('day', toDateTime64(0, 0, 'America/New_York'), toDateTime64(86400, 0, 'America/New_York')),
       age('day', toDateTime(0, 'America/New_York'), toDateTime(172800, 'America/New_York'));

DROP TABLE IF EXISTS t_rel_day_west;
CREATE TABLE t_rel_day_west (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8;
INSERT INTO t_rel_day_west SELECT toDateTime(number * 3600) FROM numbers(2000);

SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) < 1 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeDayNum(dt) < 1) FROM t_rel_day_west;
SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) = 65535 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeDayNum(dt) = 65535) FROM t_rel_day_west;
SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) IN (-2, 1023, 0) SETTINGS force_primary_key = 1;
SELECT count() FROM t_rel_day_west WHERE toMonthNumSinceEpoch(dt) < 1 SETTINGS force_primary_key = 1;
SELECT countIf(toMonthNumSinceEpoch(dt) < 1) FROM t_rel_day_west;
SELECT count() FROM t_rel_day_west WHERE toYearNumSinceEpoch(dt) < 1 SETTINGS force_primary_key = 1;
SELECT countIf(toYearNumSinceEpoch(dt) < 1) FROM t_rel_day_west;
DROP TABLE t_rel_day_west;

-- The shape the AST fuzzer found: a Nullable key and NULL in the IN set.
DROP TABLE IF EXISTS t_rel_day_west_nullable;
CREATE TABLE t_rel_day_west_nullable (dt Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY dt
    SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_rel_day_west_nullable SELECT toDateTime(number * 3600) FROM numbers(2000);
SELECT count() FROM t_rel_day_west_nullable WHERE toRelativeDayNum(dt) IN (NULL, -2, 1023, 0) SETTINGS force_primary_key = 1;
DROP TABLE t_rel_day_west_nullable;
