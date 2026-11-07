-- For a DateTime west of UTC, toRelativeDayNum, toYearNumSinceEpoch and toMonthNumSinceEpoch give 65535 on the local day
-- 1969-12-31 and 0 on the next one, so they must not be treated as monotonic across the start of local 1970-01-01.

SELECT toRelativeDayNum(toDateTime(0, 'America/New_York')), toYearNumSinceEpoch(toDateTime(0, 'America/New_York')), toMonthNumSinceEpoch(toDateTime(0, 'America/New_York'));

DROP TABLE IF EXISTS t_rel_day_west;
CREATE TABLE t_rel_day_west (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8;
INSERT INTO t_rel_day_west SELECT toDateTime(number * 3600) FROM numbers(2000);

SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) < 1 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeDayNum(dt) < 1) FROM t_rel_day_west;
SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) = 65535 SETTINGS force_primary_key = 1;
SELECT countIf(toRelativeDayNum(dt) = 65535) FROM t_rel_day_west;
SELECT count() FROM t_rel_day_west WHERE toMonthNumSinceEpoch(dt) < 1 SETTINGS force_primary_key = 1;
SELECT countIf(toMonthNumSinceEpoch(dt) < 1) FROM t_rel_day_west;
SELECT count() FROM t_rel_day_west WHERE toYearNumSinceEpoch(dt) >= 1 SETTINGS force_primary_key = 1;
SELECT countIf(toYearNumSinceEpoch(dt) >= 1) FROM t_rel_day_west;
-- Ranges after the local epoch are still pruned.
SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) = 3 SETTINGS force_primary_key = 1, max_rows_to_read = 64;

SELECT groupArray(d) FROM (SELECT toRelativeDayNum(dt) AS d FROM t_rel_day_west ORDER BY toRelativeDayNum(dt) LIMIT 3) SETTINGS optimize_read_in_order = 1;
SELECT groupArray(d) FROM (SELECT toYearNumSinceEpoch(dt) AS d FROM t_rel_day_west ORDER BY toYearNumSinceEpoch(dt) DESC LIMIT 3) SETTINGS optimize_read_in_order = 1;
SELECT count(), sum(c) FROM (SELECT toRelativeDayNum(dt) AS d, count() AS c FROM t_rel_day_west GROUP BY d) SETTINGS optimize_aggregation_in_order = 1;
SELECT count(), sum(c) FROM (SELECT toMonthNumSinceEpoch(dt) AS d, count() AS c FROM t_rel_day_west GROUP BY d) SETTINGS optimize_aggregation_in_order = 1;
SELECT groupArray(d) FROM (SELECT toRelativeDayNum(dt) AS d FROM (SELECT dt FROM t_rel_day_west ORDER BY dt) ORDER BY d LIMIT 3)
    SETTINGS optimize_sorting_by_input_stream_properties = 1, optimize_read_in_order = 0, query_plan_remove_redundant_sorting = 0;

DROP TABLE IF EXISTS t_rel_day_west_part;
CREATE TABLE t_rel_day_west_part (dt DateTime('America/New_York')) ENGINE = MergeTree PARTITION BY toRelativeDayNum(dt) ORDER BY dt;
INSERT INTO t_rel_day_west_part SELECT toDateTime(number * 3600) FROM numbers(48);
SELECT count() FROM t_rel_day_west_part WHERE dt < toDateTime(6 * 3600, 'America/New_York');
SELECT count() FROM t_rel_day_west_part WHERE dt >= toDateTime(0, 'America/New_York') AND dt < toDateTime(5 * 3600, 'America/New_York');
DROP TABLE t_rel_day_west_part;

-- The time zone argument.
DROP TABLE IF EXISTS t_rel_day_utc;
CREATE TABLE t_rel_day_utc (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8;
INSERT INTO t_rel_day_utc SELECT toDateTime(number * 3600) FROM numbers(2000);
SELECT count() FROM t_rel_day_utc WHERE toRelativeDayNum(dt, 'America/New_York') = 65535 SETTINGS force_primary_key = 1;
SELECT groupArray(d) FROM (SELECT toRelativeDayNum(dt, 'America/New_York') AS d FROM t_rel_day_utc ORDER BY toRelativeDayNum(dt, 'America/New_York') LIMIT 3) SETTINGS optimize_read_in_order = 1;
SELECT count(), sum(c) FROM (SELECT toRelativeDayNum(dt, 'America/New_York') AS d, count() AS c FROM t_rel_day_utc GROUP BY d) SETTINGS optimize_aggregation_in_order = 1;
SELECT groupArray(d) FROM (SELECT toRelativeDayNum(dt, 'America/New_York') AS d FROM (SELECT dt FROM t_rel_day_utc ORDER BY dt) ORDER BY d LIMIT 3)
    SETTINGS optimize_sorting_by_input_stream_properties = 1, optimize_read_in_order = 0, query_plan_remove_redundant_sorting = 0;
SELECT groupArray(d) FROM (SELECT toRelativeDayNum(dt, toNullable('America/New_York')) AS d FROM t_rel_day_utc ORDER BY d LIMIT 3) SETTINGS optimize_read_in_order = 1;
SELECT toRelativeDayNum(dt, NULL) AS d FROM t_rel_day_utc ORDER BY d LIMIT 1 SETTINGS optimize_read_in_order = 1;
DROP TABLE t_rel_day_utc;
DROP TABLE IF EXISTS t_rel_day_utc_key;
CREATE TABLE t_rel_day_utc_key (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY toRelativeDayNum(dt, 'America/New_York') SETTINGS index_granularity = 8;
INSERT INTO t_rel_day_utc_key SELECT toDateTime(number * 3600) FROM numbers(2000);
SELECT count() FROM t_rel_day_utc_key WHERE dt < toDateTime(6 * 3600, 'UTC');
DROP TABLE t_rel_day_utc_key;

-- East of UTC the functions stay monotonic everywhere, so a filter on dt still uses a key over them.
DROP TABLE IF EXISTS t_rel_day_east;
CREATE TABLE t_rel_day_east (dt DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY toRelativeDayNum(dt) SETTINGS index_granularity = 8;
INSERT INTO t_rel_day_east SELECT toDateTime(number * 3600) FROM numbers(2000);
SELECT count() FROM t_rel_day_east WHERE dt < toDateTime(100 * 3600, 'Asia/Tokyo') SETTINGS force_primary_key = 1;
DROP TABLE t_rel_day_east;

-- IN, also in the shape the AST fuzzer found: a Nullable key and NULL in the set.
SELECT count() FROM t_rel_day_west WHERE toRelativeDayNum(dt) IN (-2, 1023, 0) SETTINGS force_primary_key = 1;
DROP TABLE t_rel_day_west;
DROP TABLE IF EXISTS t_rel_day_west_nullable;
CREATE TABLE t_rel_day_west_nullable (dt Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY dt
    SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_rel_day_west_nullable SELECT toDateTime(number * 3600) FROM numbers(2000);
SELECT count() FROM t_rel_day_west_nullable WHERE toRelativeDayNum(dt) IN (NULL, -2, 1023, 0) SETTINGS force_primary_key = 1;
DROP TABLE t_rel_day_west_nullable;
