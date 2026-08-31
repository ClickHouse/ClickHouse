-- https://github.com/ClickHouse/ClickHouse/issues/116931
-- https://github.com/ClickHouse/ClickHouse/issues/116935
-- https://github.com/ClickHouse/ClickHouse/issues/116828
-- https://github.com/ClickHouse/ClickHouse/issues/103686
-- `toString` of a date-time is not injective in a time zone with a UTC offset transition: in a
-- fall-back hour two distinct instants render to the same wall-clock string. `toFixedString` of a
-- `String` is not injective either: the value is NUL-padded, so 'a' and 'a\0' give the same
-- `FixedString(2)`. Every pair below prints the result at the default settings and then with the
-- optimization that trusted the claim disabled; the two must agree.

DROP TABLE IF EXISTS t_tz_fold;
CREATE TABLE t_tz_fold (dt DateTime('Europe/Amsterdam'), v UInt32) ENGINE = Memory;
-- 2018-10-28 in Amsterdam: 03:00 CEST falls back to 02:00 CET.
INSERT INTO t_tz_fold VALUES (1540686600, 2), (1540690200, 1);

SELECT 'ground truth';
SELECT toString(toDateTime(1540686600, 'Europe/Amsterdam')) = toString(toDateTime(1540690200, 'Europe/Amsterdam'));
SELECT toFixedString('a', 2) = toFixedString('a\0', 2);

SELECT 'uniq elimination';
SELECT uniqExact(toString(dt)) FROM t_tz_fold;
SELECT uniqExact(toString(dt)) FROM t_tz_fold SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'order by truncation';
SELECT toString(dt) AS s, max(v) AS m FROM t_tz_fold GROUP BY dt ORDER BY s, m DESC;
SELECT toString(dt) AS s, max(v) AS m FROM t_tz_fold GROUP BY dt ORDER BY s, m DESC SETTINGS optimize_truncate_order_by_after_group_by_keys = 0;

SELECT 'group by';
SELECT count() FROM (SELECT toString(dt) AS s FROM t_tz_fold GROUP BY s);
SELECT count() FROM (SELECT toString(dt) AS s FROM t_tz_fold GROUP BY s) SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT count() FROM (SELECT 1 FROM (SELECT arrayJoin(['a', 'a\0']) AS x) GROUP BY toFixedString(x, 2));
SELECT count() FROM (SELECT 1 FROM (SELECT arrayJoin(['a', 'a\0']) AS x) GROUP BY toFixedString(x, 2)) SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT 'limit by';
SELECT count() FROM (SELECT toString(dt) AS s, v FROM t_tz_fold ORDER BY v LIMIT 1 BY s);
SELECT count() FROM (SELECT toString(dt) AS s, v FROM t_tz_fold ORDER BY v LIMIT 1 BY s) SETTINGS optimize_injective_functions_in_limit_by = 0;
SELECT count() FROM (SELECT arrayJoin(['a', 'a\0']) AS x LIMIT 1 BY toFixedString(x, 2));
SELECT count() FROM (SELECT arrayJoin(['a', 'a\0']) AS x LIMIT 1 BY toFixedString(x, 2)) SETTINGS optimize_injective_functions_in_limit_by = 0;

SELECT 'independent window partitions';
DROP TABLE IF EXISTS t_window_fold;
CREATE TABLE t_window_fold (ts DateTime('America/New_York'), v UInt64) ENGINE = MergeTree ORDER BY tuple()
PARTITION BY intDiv(toUnixTimestamp(ts), 3600);
-- 1762061400 is 2025-11-02 01:30:00 EDT; 3600 s later is 01:30:00 EST - another hourly partition
-- with the same rendering.
INSERT INTO t_window_fold SELECT toDateTime(1762061400 + 3600 * intDiv(number, 100), 'America/New_York'), number FROM numbers(1600);

SELECT c FROM (SELECT DISTINCT toString(ts) AS s, count() OVER (PARTITION BY toString(ts)) AS c FROM t_window_fold) WHERE s = '2025-11-02 01:30:00' SETTINGS max_threads = 16;
SELECT c FROM (SELECT DISTINCT toString(ts) AS s, count() OVER (PARTITION BY toString(ts)) AS c FROM t_window_fold) WHERE s = '2025-11-02 01:30:00' SETTINGS max_threads = 16, allow_window_partitions_independently = 0;

DROP TABLE IF EXISTS t_window_pad;
CREATE TABLE t_window_pad (s String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY length(s);
INSERT INTO t_window_pad SELECT concat('k', repeat('\0', number % 16)) FROM numbers(1600);

SELECT DISTINCT count() OVER (PARTITION BY toFixedString(s, 16)) FROM t_window_pad SETTINGS max_threads = 16;
SELECT DISTINCT count() OVER (PARTITION BY toFixedString(s, 16)) FROM t_window_pad SETTINGS max_threads = 16, allow_window_partitions_independently = 0;

SELECT 'a fixed-offset time zone is still injective';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT uniqExact(toString(toDateTime(number, 'UTC'))) FROM numbers(2)) WHERE explain LIKE '%uniqExact%' AND explain NOT LIKE '%toString%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT uniqExact(toString(toDateTime(number, 'Europe/Amsterdam'))) FROM numbers(2)) WHERE explain LIKE '%toString%';

DROP TABLE t_tz_fold;
DROP TABLE t_window_fold;
DROP TABLE t_window_pad;
