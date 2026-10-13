-- `INTERPOLATE` keeps every column of the previous step alive, which used to include the `Set`
-- placeholder of an `IN` predicate. That placeholder is not a value: carried past the filter that
-- consumes it, it reaches steps that build rows out of the whole header, and the `FINAL` merge failed
-- with 246 `CORRUPTED_DATA` ("Cannot get value from Set").

DROP TABLE IF EXISTS t_interpolate_dict_source;
DROP DICTIONARY IF EXISTS d_interpolate;
DROP TABLE IF EXISTS t_interpolate_coalescing;
DROP TABLE IF EXISTS t_interpolate_in;

CREATE TABLE t_interpolate_dict_source (id UInt64, val UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_interpolate_dict_source SELECT number, toUInt32(number % 97) FROM numbers(500);

CREATE DICTIONARY d_interpolate (id UInt64, val UInt32 DEFAULT 0)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_interpolate_dict_source' DB currentDatabase()))
LIFETIME(0)
LAYOUT(FLAT());

CREATE TABLE t_interpolate_coalescing (k UInt32, a Nullable(Int64), b Nullable(Int64))
ENGINE = CoalescingMergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_interpolate_coalescing SELECT number, toInt64(number) * 2 - 100, NULL FROM numbers(400);
-- A second unmerged part, so that FINAL actually merges.
INSERT INTO t_interpolate_coalescing SELECT number, NULL, toInt64(number) * 3 FROM numbers(600) SETTINGS optimize_on_insert = 0;

SELECT 'a FINAL read whose filter is an IN set moved into PREWHERE';
SELECT count() FROM (
    SELECT a, k FROM t_interpolate_coalescing FINAL
    WHERE dictGet(currentDatabase() || '.d_interpolate', 'val', toUInt64(k) % 500) >= 6
    ORDER BY k WITH FILL FROM 0 TO 38 INTERPOLATE (a AS 7))
SETTINGS optimize_move_to_prewhere_if_final = 1;
SELECT count() FROM (
    SELECT a, k FROM t_interpolate_coalescing FINAL
    WHERE dictGet(currentDatabase() || '.d_interpolate', 'val', toUInt64(k) % 500) >= 6
    ORDER BY k WITH FILL FROM 0 TO 38 INTERPOLATE (a AS 7))
SETTINGS optimize_move_to_prewhere_if_final = 0;
SELECT count() FROM (
    SELECT a, k FROM t_interpolate_coalescing FINAL
    WHERE dictGet(currentDatabase() || '.d_interpolate', 'val', toUInt64(k) % 500) >= 6
    ORDER BY k WITH FILL FROM 0 TO 38 INTERPOLATE (a AS 7))
SETTINGS optimize_move_to_prewhere_if_final = 1, optimize_inverse_dictionary_lookup = 0;

SELECT 'a plain IN filter with WITH FILL INTERPOLATE';
CREATE TABLE t_interpolate_in (k UInt32, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_interpolate_in VALUES (1, '8'), (2, '8');
SELECT k, s FROM t_interpolate_in WHERE s IN ('8') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS '7');

SELECT 'INTERPOLATE of a column the filter reads';
SELECT k, s FROM t_interpolate_in WHERE s IN ('8') ORDER BY k WITH FILL FROM 0 TO 4 INTERPOLATE (s AS s || 'x');

DROP TABLE t_interpolate_in;
DROP TABLE t_interpolate_coalescing;
DROP DICTIONARY d_interpolate;
DROP TABLE t_interpolate_dict_source;
