-- Hash joins on 8-bit and 16-bit keys use a fixed-size map. With the serial layout it is one map
-- (`key8`, `key16`). With the parallel layout it is split into buckets (`two_level_key8`,
-- `two_level_key16`). Both layouts must return the same rows for INNER, RIGHT and FULL joins, so each
-- serial result is printed and the parallel result is compared with it. After a parallel build, the
-- map of a `UInt32` or `Int32` key with values in a small range becomes a one-bucket range map. This
-- conversion must not change the result. In both layouts, the fixed-size map serves as a runtime
-- filter only when `join_runtime_filter_from_fixed_hash_table` is on.
-- `parallel_hash_join_threshold = 1000000000` builds the serial layout and `0` the parallel one.

DROP TABLE IF EXISTS t_u8_l;
DROP TABLE IF EXISTS t_u8_r;
DROP TABLE IF EXISTS t_i8_l;
DROP TABLE IF EXISTS t_i8_r;
DROP TABLE IF EXISTS t_u16_l;
DROP TABLE IF EXISTS t_u16_r;
DROP TABLE IF EXISTS t_i16_l;
DROP TABLE IF EXISTS t_i16_r;
DROP TABLE IF EXISTS t_sparse_l;
DROP TABLE IF EXISTS t_sparse_r;
DROP TABLE IF EXISTS t_range_l;
DROP TABLE IF EXISTS t_range_r;
DROP TABLE IF EXISTS t_range_i32_l;
DROP TABLE IF EXISTS t_range_i32_r;
DROP TABLE IF EXISTS t_rf_l;
DROP TABLE IF EXISTS t_rf_r;

CREATE TABLE t_u8_l (k UInt8, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_u8_r (k UInt8, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_u8_l VALUES (0, 'l0'), (1, 'l1'), (2, 'l2'), (200, 'l200'), (255, 'l255');
INSERT INTO t_u8_r VALUES (0, 'r0'), (2, 'r2'), (2, 'r2b'), (200, 'r200'), (255, 'r255'), (3, 'r3');

CREATE TABLE t_i8_l (k Int8, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_i8_r (k Int8, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_i8_l VALUES (-128, 'ln'), (-1, 'lm'), (0, 'l0'), (127, 'lp');
INSERT INTO t_i8_r VALUES (-128, 'rn'), (0, 'r0'), (127, 'rp'), (1, 'r1');

CREATE TABLE t_u16_l (k UInt16, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_u16_r (k UInt16, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_u16_l SELECT number * 10, 'l' || toString(number * 10) FROM numbers(20);
INSERT INTO t_u16_r SELECT number * 15, 'r' || toString(number * 15) FROM numbers(20);

CREATE TABLE t_i16_l (k Int16, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_i16_r (k Int16, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_i16_l VALUES (-30000, 'ln'), (-1, 'lm'), (0, 'l0'), (30000, 'lp');
INSERT INTO t_i16_r VALUES (-30000, 'rn'), (0, 'r0'), (30000, 'rp'), (5, 'r5');

CREATE TABLE t_sparse_l (k UInt16, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_sparse_r (k UInt16, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sparse_l VALUES (0, 'l0'), (1, 'l1'), (2, 'l2');
INSERT INTO t_sparse_r VALUES (0, 'r0'), (1, 'r1'), (50000, 'r50000'), (65535, 'r65535');

CREATE TABLE t_range_l (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_range_r (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_range_l SELECT number, 'l' || toString(number) FROM numbers(200);
INSERT INTO t_range_r SELECT number * 2, 'r' || toString(number * 2) FROM numbers(80);

CREATE TABLE t_range_i32_l (k Int32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_range_i32_r (k Int32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_range_i32_l SELECT number - 50, 'l' || toString(number - 50) FROM numbers(120);
INSERT INTO t_range_i32_r SELECT (number - 20) * 2, 'r' || toString((number - 20) * 2) FROM numbers(40);

CREATE TABLE t_rf_l (k UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_r (k UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rf_r SELECT toUInt8(number) FROM numbers(50);
INSERT INTO t_rf_l SELECT toUInt8(number % 100) FROM numbers(5000);

SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_analyzer = 1;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_join_fixed_hash_table_conversion = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET join_use_nulls = 1;

SELECT '-- key8 uint8 inner';
SELECT l.k, l.v, r.v FROM t_u8_l AS l INNER JOIN t_u8_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04891_key8_serial';
SELECT
    (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_u8_l AS l INNER JOIN t_u8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_u8_l AS l INNER JOIN t_u8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000)
SETTINGS log_comment = '04891_key8_compare';

SELECT '-- key8 uint8 right';
SELECT r.k, l.v, r.v FROM t_u8_l AS l RIGHT JOIN t_u8_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_u8_l AS l RIGHT JOIN t_u8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_u8_l AS l RIGHT JOIN t_u8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- key8 uint8 full';
SELECT l.k, r.k, l.v, r.v FROM t_u8_l AS l FULL JOIN t_u8_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((l.k, r.k, l.v, r.v))) FROM t_u8_l AS l FULL JOIN t_u8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, r.k, l.v, r.v))) FROM t_u8_l AS l FULL JOIN t_u8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- key8 int8 inner';
SELECT l.k, l.v, r.v FROM t_i8_l AS l INNER JOIN t_i8_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_i8_l AS l INNER JOIN t_i8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_i8_l AS l INNER JOIN t_i8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- key8 int8 right';
SELECT r.k, l.v, r.v FROM t_i8_l AS l RIGHT JOIN t_i8_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_i8_l AS l RIGHT JOIN t_i8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_i8_l AS l RIGHT JOIN t_i8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- key8 int8 full';
SELECT l.k, r.k, l.v, r.v FROM t_i8_l AS l FULL JOIN t_i8_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((l.k, r.k, l.v, r.v))) FROM t_i8_l AS l FULL JOIN t_i8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, r.k, l.v, r.v))) FROM t_i8_l AS l FULL JOIN t_i8_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- key16 uint16 inner';
SELECT l.k, l.v, r.v FROM t_u16_l AS l INNER JOIN t_u16_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04891_key16_serial';
SELECT
    (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_u16_l AS l INNER JOIN t_u16_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_u16_l AS l INNER JOIN t_u16_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000)
SETTINGS log_comment = '04891_key16_compare';

SELECT '-- key16 int16 inner';
SELECT l.k, l.v, r.v FROM t_i16_l AS l INNER JOIN t_i16_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_i16_l AS l INNER JOIN t_i16_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_i16_l AS l INNER JOIN t_i16_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- sparse key16 right';
SELECT r.k, l.v, r.v FROM t_sparse_l AS l RIGHT JOIN t_sparse_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_sparse_l AS l RIGHT JOIN t_sparse_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_sparse_l AS l RIGHT JOIN t_sparse_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT '-- sparse key16 full';
SELECT l.k, r.k, l.v, r.v FROM t_sparse_l AS l FULL JOIN t_sparse_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT
    (SELECT arraySort(groupArray((l.k, r.k, l.v, r.v))) FROM t_sparse_l AS l FULL JOIN t_sparse_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
    = (SELECT arraySort(groupArray((l.k, r.k, l.v, r.v))) FROM t_sparse_l AS l FULL JOIN t_sparse_r AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 1000000000);

SELECT count() FROM t_range_l AS l INNER JOIN t_range_r AS r ON l.k = r.k FORMAT Null
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04891_range_u32';

SELECT '-- range uint32 inner conversion on vs off';
SELECT
    (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_range_l AS l INNER JOIN t_range_r AS r ON l.k = r.k
     SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1)
    = (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_range_l AS l INNER JOIN t_range_r AS r ON l.k = r.k
       SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 0);

SELECT '-- range uint32 right conversion on vs off';
SELECT
    (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_range_l AS l RIGHT JOIN t_range_r AS r ON l.k = r.k
     SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1)
    = (SELECT arraySort(groupArray((r.k, l.v, r.v))) FROM t_range_l AS l RIGHT JOIN t_range_r AS r ON l.k = r.k
       SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 0);

SELECT count() FROM t_range_i32_l AS l INNER JOIN t_range_i32_r AS r ON l.k = r.k FORMAT Null
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04891_range_i32';

SELECT '-- range int32 inner conversion on vs off';
SELECT
    (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_range_i32_l AS l INNER JOIN t_range_i32_r AS r ON l.k = r.k
     SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1)
    = (SELECT arraySort(groupArray((l.k, l.v, r.v))) FROM t_range_i32_l AS l INNER JOIN t_range_i32_r AS r ON l.k = r.k
       SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 0);

SELECT '-- shared rf key8 serial';
SELECT 'rf0', count() FROM t_rf_l AS l INNER JOIN t_rf_r AS r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 1000000000, join_runtime_filter_from_fixed_hash_table = 0, log_comment = '04891_rf_serial_off';
SELECT 'rf1', count() FROM t_rf_l AS l INNER JOIN t_rf_r AS r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 1000000000, join_runtime_filter_from_fixed_hash_table = 1, log_comment = '04891_rf_serial_on';

SELECT '-- shared rf key8 parallel';
SELECT 'rf0', count() FROM t_rf_l AS l INNER JOIN t_rf_r AS r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 0, join_runtime_filter_from_fixed_hash_table = 0, log_comment = '04891_rf_parallel_off';
SELECT 'rf1', count() FROM t_rf_l AS l INNER JOIN t_rf_r AS r ON l.k = r.k
SETTINGS parallel_hash_join_threshold = 0, join_runtime_filter_from_fixed_hash_table = 1, log_comment = '04891_rf_parallel_on';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT '-- per query: map types built, type converted to, runtime filter published';
SELECT
    q.log_comment,
    arraySort(groupUniqArrayIf(extract(t.message, 'Join hash table type: (\\w+)'), t.message LIKE '%Join hash table type: %')) AS built,
    arraySort(groupUniqArrayIf(extract(t.message, 'type: (\\w+)\\)'), t.message LIKE '%Converted join hash map to fixed hash map%')) AS converted,
    countIf(t.message LIKE '%Published shared fixed-hash-table runtime filter%') > 0 AS filter_published
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
          AND match(log_comment, '^04891_(key|range|rf)')
) AS q ON t.query_id = q.query_id
WHERE t.event_date >= yesterday() AND t.event_time >= now() - 600
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE t_u8_l;
DROP TABLE t_u8_r;
DROP TABLE t_i8_l;
DROP TABLE t_i8_r;
DROP TABLE t_u16_l;
DROP TABLE t_u16_r;
DROP TABLE t_i16_l;
DROP TABLE t_i16_r;
DROP TABLE t_sparse_l;
DROP TABLE t_sparse_r;
DROP TABLE t_range_l;
DROP TABLE t_range_r;
DROP TABLE t_range_i32_l;
DROP TABLE t_range_i32_r;
DROP TABLE t_rf_l;
DROP TABLE t_rf_r;
