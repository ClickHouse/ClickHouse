-- After the build, a `partitioned_hash` join converts a dense 32- or 64-bit integer table to a fixed hash map, as
-- `hash` does, and publishes the exact runtime filter of a fixed table (8- or 16-bit keys, or a converted range) in
-- place of the planner's Bloom filter. The conversion and the publication are visible in the text log; the results
-- must not depend on either.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right_i32;
DROP TABLE IF EXISTS t_right_i64;
DROP TABLE IF EXISTS t_right_neg;
DROP TABLE IF EXISTS t_rf_probe;
DROP TABLE IF EXISTS t_rf_build_u8;
DROP TABLE IF EXISTS t_rf_build_i32;

CREATE TABLE t_left (id Int32, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_i32 (id Int32, rval String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_i64 (id Int64, rval String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_neg (id Int32, rval String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left SELECT number - 5, 'l' || toString(number - 5) FROM numbers(10);
INSERT INTO t_right_i32 VALUES (0, 'r0'), (2, 'r2'), (4, 'r4');
INSERT INTO t_right_i64 VALUES (0, 'r0'), (2, 'r2'), (4, 'r4');
INSERT INTO t_right_neg VALUES (-2, 'r-2'), (0, 'r0'), (2, 'r2');

-- 5000 probe rows over keys 0..99; the build sides hold keys 0..49 (a native 8-bit table) and 0..99 as Int32
-- (a table the conversion turns into a range map), so half of the probe rows match.
CREATE TABLE t_rf_probe (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_build_u8 (k UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_build_i32 (k Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_probe SELECT toInt32(number % 100) FROM numbers(5000);
INSERT INTO t_rf_build_u8 SELECT toUInt8(number) FROM numbers(50);
INSERT INTO t_rf_build_i32 SELECT toInt32(number * 2) FROM numbers(50);

SET enable_analyzer = 1;
SET join_algorithm = 'partitioned_hash';
SET enable_join_fixed_hash_table_conversion = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_read_in_order_through_join = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;

SELECT '-- conversion Int32';
SELECT count(*) FROM t_left JOIN t_right_i32 ON t_left.id = t_right_i32.id FORMAT NULL SETTINGS log_comment = '05228 convert i32';
SELECT '-- conversion Int64';
SELECT count(*) FROM t_left JOIN t_right_i64 ON t_left.id = t_right_i64.id FORMAT NULL SETTINGS log_comment = '05228 convert i64';
SELECT '-- conversion negative range';
SELECT count(*) FROM t_left JOIN t_right_neg ON t_left.id = t_right_neg.id FORMAT NULL SETTINGS log_comment = '05228 convert neg';
SELECT '-- no conversion when the setting is off';
SELECT count(*) FROM t_left JOIN t_right_i32 ON t_left.id = t_right_i32.id FORMAT NULL SETTINGS log_comment = '05228 convert off', enable_join_fixed_hash_table_conversion = 0;

SYSTEM FLUSH LOGS query_log, text_log;

-- One row per build query: converted for the three dense tables, not converted with the setting off.
SELECT q.log_comment, countIf(t.query_id != '') > 0 AS converted
FROM system.query_log AS q
LEFT JOIN
(
    SELECT query_id FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND message LIKE '%Converted join hash map to fixed hash map%' AND message LIKE '%type: range8%'
) AS t ON q.query_id = t.query_id
WHERE q.event_date >= yesterday() AND q.event_time >= now() - 600 AND q.current_database = currentDatabase()
      AND q.type = 'QueryFinish' AND q.log_comment LIKE '05228 convert %'
GROUP BY q.log_comment
ORDER BY q.log_comment;

SELECT '-- ALL INNER Int32, conversion on and off';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;
SELECT '-- ALL INNER Int64';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i64 ON t_left.id = t_right_i64.id ORDER BY t_left.id;
SELECT '-- ALL INNER negative';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_neg ON t_left.id = t_right_neg.id ORDER BY t_left.id;
SELECT '-- ALL LEFT Int32';
SELECT t_left.id, val, rval FROM t_left ALL LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- ALL RIGHT Int32';
SELECT t_left.id, val, rval FROM t_left ALL RIGHT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- ALL FULL Int32';
SELECT t_left.id, t_right_i32.id, val, rval FROM t_left ALL FULL JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id, t_right_i32.id;
SELECT '-- ANY INNER Int32';
SELECT t_left.id, val, rval FROM t_left ANY INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- ANY LEFT Int32';
SELECT t_left.id, val, rval FROM t_left ANY LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- ANY RIGHT Int32';
SELECT t_left.id, val, rval FROM t_left ANY RIGHT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- SEMI LEFT Int32';
SELECT t_left.id, val FROM t_left SEMI LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- SEMI RIGHT Int32';
SELECT t_right_i32.id, rval FROM t_left SEMI RIGHT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_right_i32.id;
SELECT '-- ANTI LEFT Int32';
SELECT t_left.id, val FROM t_left ANTI LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT '-- ANTI RIGHT Int32';
SELECT t_right_i32.id, rval FROM t_left ANTI RIGHT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_right_i32.id;

SELECT '-- the exact runtime filter of a fixed table';
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SELECT 'u8 on', count() FROM t_rf_probe AS p INNER JOIN t_rf_build_u8 AS b ON p.k = b.k
SETTINGS join_runtime_filter_from_fixed_hash_table = 1, log_comment = '05228 rf u8 on';
SELECT 'u8 off', count() FROM t_rf_probe AS p INNER JOIN t_rf_build_u8 AS b ON p.k = b.k
SETTINGS join_runtime_filter_from_fixed_hash_table = 0, log_comment = '05228 rf u8 off';
SELECT 'range on', count() FROM t_rf_probe AS p INNER JOIN t_rf_build_i32 AS b ON p.k = b.k
SETTINGS join_runtime_filter_from_fixed_hash_table = 1, log_comment = '05228 rf range on';
SELECT 'range off', count() FROM t_rf_probe AS p INNER JOIN t_rf_build_i32 AS b ON p.k = b.k
SETTINGS join_runtime_filter_from_fixed_hash_table = 0, log_comment = '05228 rf range off';
SELECT 'range on, conversion off', count() FROM t_rf_probe AS p INNER JOIN t_rf_build_i32 AS b ON p.k = b.k
SETTINGS join_runtime_filter_from_fixed_hash_table = 1, enable_join_fixed_hash_table_conversion = 0, log_comment = '05228 rf range unconverted';

SYSTEM FLUSH LOGS query_log, text_log;

-- Published only with the setting on and only from a fixed table: the native 8-bit table and the converted range,
-- not the unconverted 32-bit table.
SELECT q.log_comment, countIf(t.query_id != '') > 0 AS published
FROM system.query_log AS q
LEFT JOIN
(
    SELECT query_id FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND message LIKE '%Published shared fixed-hash-table runtime filter%'
) AS t ON q.query_id = t.query_id
WHERE q.event_date >= yesterday() AND q.event_time >= now() - 600 AND q.current_database = currentDatabase()
      AND q.type = 'QueryFinish' AND q.log_comment LIKE '05228 rf %'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE t_left;
DROP TABLE t_right_i32;
DROP TABLE t_right_i64;
DROP TABLE t_right_neg;
DROP TABLE t_rf_probe;
DROP TABLE t_rf_build_u8;
DROP TABLE t_rf_build_i32;
