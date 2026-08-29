-- Test for the `apply_string_filters_during_scan` setting: substring search conditions from PREWHERE
-- are pushed down into the column scan, and values that do not match them are read as empty strings.
-- The result of every query must be the same with the setting enabled and disabled.

DROP TABLE IF EXISTS t_string_filter_wide;
DROP TABLE IF EXISTS t_string_filter_compact;
DROP TABLE IF EXISTS t_string_filter_single;

-- The default serialization of String columns (with a separate stream for sizes).
CREATE TABLE t_string_filter_wide (id UInt32, s String, n Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

CREATE TABLE t_string_filter_compact (id UInt32, s String, n Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 1.0;

-- The legacy serialization of String columns (with inline sizes).
CREATE TABLE t_string_filter_single (id UInt32, s String, n Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'single_stream', ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_wide
SELECT
    number,
    multiIf(
        number % 11 = 0, '',
        number % 7 = 0, 'lorem ipsum needle dolor ' || toString(number),
        number % 5 = 0, 'needle at the start ' || toString(number),
        number % 3 = 0, toString(number) || ' ends with needle',
        'nothing interesting ' || toString(number)),
    multiIf(
        number % 13 = 0, NULL,
        number % 7 = 0, 'nullable needle ' || toString(number),
        'plain value ' || toString(number))
FROM numbers(100000);

INSERT INTO t_string_filter_compact SELECT * FROM t_string_filter_wide;
INSERT INTO t_string_filter_single SELECT * FROM t_string_filter_wide;

SELECT 'like substring';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_string_filter_compact PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_string_filter_compact PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_string_filter_single PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_string_filter_single PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_single PREWHERE startsWith(s, 'needle at') AND n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_single PREWHERE startsWith(s, 'needle at') AND n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;

SELECT 'like prefix and suffix';
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE 'needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE 'needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%needle' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%needle' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE 'needle%start%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE 'needle%start%' SETTINGS apply_string_filters_during_scan = 1;

SELECT 'startsWith, endsWith, position, equality';
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE startsWith(s, 'needle at') SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE startsWith(s, 'needle at') SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE endsWith(s, 'with needle') SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE endsWith(s, 'with needle') SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') > 0 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') > 0 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') = 21 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') = 21 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s = 'needle at the start 5' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s = 'needle at the start 5' SETTINGS apply_string_filters_during_scan = 1;

SELECT 'conditions that must not be pushed down';
-- The result must not contain replaced values: these conditions can match values without the needle (or an empty string).
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') = 0 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE position(s, 'needle') = 0 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE NOT (s LIKE '%needle%') SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE NOT (s LIKE '%needle%') SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' OR id = 1 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' OR id = 1 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count() FROM t_string_filter_wide PREWHERE s = '' SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_wide PREWHERE s = '' SETTINGS apply_string_filters_during_scan = 1;

SELECT 'AND chains and multiple columns';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND id % 2 = 0 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND id % 2 = 0 SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND s LIKE '%ipsum%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND s LIKE '%ipsum%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND length(s) < 100 SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE s LIKE '%needle%' AND length(s) < 100 SETTINGS apply_string_filters_during_scan = 1;

SELECT 'Nullable column';
SELECT count(), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(n)) FROM t_string_filter_wide PREWHERE n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count(), sum(cityHash64(n)) FROM t_string_filter_compact PREWHERE n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(n)) FROM t_string_filter_compact PREWHERE n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;

SELECT 'escaped LIKE patterns';
SELECT count() FROM t_string_filter_wide PREWHERE s LIKE '%needle\\_at%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_wide PREWHERE s LIKE '%needle\\_at%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count() FROM t_string_filter_wide PREWHERE s LIKE '%needle_at%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_wide PREWHERE s LIKE '%needle_at%' SETTINGS apply_string_filters_during_scan = 1;

SELECT 'values in the result';
SELECT id, s, n FROM t_string_filter_wide PREWHERE s LIKE '%needle%' ORDER BY id LIMIT 5 SETTINGS apply_string_filters_during_scan = 0;
SELECT id, s, n FROM t_string_filter_wide PREWHERE s LIKE '%needle%' ORDER BY id LIMIT 5 SETTINGS apply_string_filters_during_scan = 1;
SELECT s, count() FROM t_string_filter_wide PREWHERE endsWith(s, 'needle') GROUP BY s ORDER BY s LIMIT 3 SETTINGS apply_string_filters_during_scan = 0;
SELECT s, count() FROM t_string_filter_wide PREWHERE endsWith(s, 'needle') GROUP BY s ORDER BY s LIMIT 3 SETTINGS apply_string_filters_during_scan = 1;

SELECT 'long values and needle crossing value boundaries';
DROP TABLE IF EXISTS t_string_filter_long;
CREATE TABLE t_string_filter_long (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
-- Some values are larger than the read buffer, and the values of two adjacent rows contain
-- the needle only across the boundary between them (`need` + `le...`), which must not match.
INSERT INTO t_string_filter_long SELECT number,
    multiIf(
        number % 10 = 0, repeat('0123456789', 300000) || 'needle' || repeat('y', 100),
        number % 10 = 2, 'need',
        number % 10 = 3, 'le' || toString(number),
        number % 10 = 4, '',
        repeat('z', 100) || toString(number))
FROM numbers(100);
SELECT count(), sum(length(s)), sum(cityHash64(s)) FROM t_string_filter_long PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(length(s)), sum(cityHash64(s)) FROM t_string_filter_long PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT count() FROM t_string_filter_long PREWHERE s LIKE '%needle%' OR s LIKE '%le3%' SETTINGS apply_string_filters_during_scan = 1;
DROP TABLE t_string_filter_long;

SELECT 'the optimization is applied';
SELECT count() > 0 FROM t_string_filter_wide PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05028_string_filter_applied';
SELECT count() > 0 FROM t_string_filter_single PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05028_string_filter_applied';
SYSTEM FLUSH LOGS query_log;
SELECT
    sum(ProfileEvents['StringValueFilterValuesChecked']) > 0,
    sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0,
    sum(ProfileEvents['StringValueFilterBytesSkipped']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05028_string_filter_applied';

DROP TABLE t_string_filter_wide;
DROP TABLE t_string_filter_compact;
DROP TABLE t_string_filter_single;
