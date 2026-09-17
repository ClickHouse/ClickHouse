-- Tags: no-random-settings
-- Serial `key8`/`key16` use a 1-bucket `JoinFixedHashMap`. Parallel fill uses
-- `two_level_key8`/`two_level_key16` (256 virtual buckets). Range maps after conversion stay
-- 1-bucket. Layout is pinned by `parallel_hash_join_threshold`, not by `join_algorithm`.

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
SET enable_join_fixed_hash_table_conversion = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET join_use_nulls = 1;

SELECT '-- key8 uint8 inner serial';
SELECT l.k, l.v, r.v FROM t_u8_l AS l INNER JOIN t_u8_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04891_key8_serial';
SELECT '-- key8 uint8 inner parallel';
SELECT l.k, l.v, r.v FROM t_u8_l AS l INNER JOIN t_u8_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04891_key8_parallel';

SELECT '-- key8 uint8 right serial';
SELECT r.k, l.v, r.v FROM t_u8_l AS l RIGHT JOIN t_u8_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- key8 uint8 right parallel';
SELECT r.k, l.v, r.v FROM t_u8_l AS l RIGHT JOIN t_u8_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- key8 uint8 full serial';
SELECT l.k, r.k, l.v, r.v FROM t_u8_l AS l FULL JOIN t_u8_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- key8 uint8 full parallel';
SELECT l.k, r.k, l.v, r.v FROM t_u8_l AS l FULL JOIN t_u8_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- key8 int8 inner serial';
SELECT l.k, l.v, r.v FROM t_i8_l AS l INNER JOIN t_i8_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- key8 int8 inner parallel';
SELECT l.k, l.v, r.v FROM t_i8_l AS l INNER JOIN t_i8_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- key8 int8 right serial';
SELECT r.k, l.v, r.v FROM t_i8_l AS l RIGHT JOIN t_i8_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- key8 int8 right parallel';
SELECT r.k, l.v, r.v FROM t_i8_l AS l RIGHT JOIN t_i8_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- key8 int8 full serial';
SELECT l.k, r.k, l.v, r.v FROM t_i8_l AS l FULL JOIN t_i8_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- key8 int8 full parallel';
SELECT l.k, r.k, l.v, r.v FROM t_i8_l AS l FULL JOIN t_i8_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- key16 uint16 inner serial';
SELECT l.k, l.v, r.v FROM t_u16_l AS l INNER JOIN t_u16_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04891_key16_serial';
SELECT '-- key16 uint16 inner parallel';
SELECT l.k, l.v, r.v FROM t_u16_l AS l INNER JOIN t_u16_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0, log_comment = '04891_key16_parallel';

SELECT '-- key16 int16 inner serial';
SELECT l.k, l.v, r.v FROM t_i16_l AS l INNER JOIN t_i16_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- key16 int16 inner parallel';
SELECT l.k, l.v, r.v FROM t_i16_l AS l INNER JOIN t_i16_r AS r ON l.k = r.k ORDER BY l.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- sparse key16 right serial';
SELECT r.k, l.v, r.v FROM t_sparse_l AS l RIGHT JOIN t_sparse_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- sparse key16 right parallel';
SELECT r.k, l.v, r.v FROM t_sparse_l AS l RIGHT JOIN t_sparse_r AS r ON l.k = r.k ORDER BY r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- sparse key16 full serial';
SELECT l.k, r.k, l.v, r.v FROM t_sparse_l AS l FULL JOIN t_sparse_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 1000000000;
SELECT '-- sparse key16 full parallel';
SELECT l.k, r.k, l.v, r.v FROM t_sparse_l AS l FULL JOIN t_sparse_r AS r ON l.k = r.k ORDER BY l.k, r.k, l.v, r.v
SETTINGS parallel_hash_join_threshold = 0;

SELECT '-- range conversion after parallel key32';
SELECT count(*) FROM t_range_l AS l INNER JOIN t_range_r AS r ON l.k = r.k FORMAT NULL
SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1, log_comment = '04891_range_u32';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0 AS triggered
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_range_u32' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      )
      AND message LIKE '%Converted join hash map to fixed hash map%'
      AND message LIKE '%type: range%'
      AND message NOT LIKE '%two_level%';

SELECT '-- range uint32 inner conversion on vs off';
SELECT
    (
        SELECT groupArray((k, lv, rv))
        FROM
        (
            SELECT l.k AS k, l.v AS lv, r.v AS rv
            FROM t_range_l AS l INNER JOIN t_range_r AS r ON l.k = r.k
            ORDER BY l.k, l.v, r.v
            SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1
        )
    ) = (
        SELECT groupArray((k, lv, rv))
        FROM
        (
            SELECT l.k AS k, l.v AS lv, r.v AS rv
            FROM t_range_l AS l INNER JOIN t_range_r AS r ON l.k = r.k
            ORDER BY l.k, l.v, r.v
            SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 0
        )
    );

SELECT '-- range uint32 right conversion on vs off';
SELECT
    (
        SELECT groupArray((k, lv, rv))
        FROM
        (
            SELECT r.k AS k, l.v AS lv, r.v AS rv
            FROM t_range_l AS l RIGHT JOIN t_range_r AS r ON l.k = r.k
            ORDER BY r.k, l.v, r.v
            SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1
        )
    ) = (
        SELECT groupArray((k, lv, rv))
        FROM
        (
            SELECT r.k AS k, l.v AS lv, r.v AS rv
            FROM t_range_l AS l RIGHT JOIN t_range_r AS r ON l.k = r.k
            ORDER BY r.k, l.v, r.v
            SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 0
        )
    );

SELECT count(*) FROM t_range_i32_l AS l INNER JOIN t_range_i32_r AS r ON l.k = r.k FORMAT NULL
SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1, log_comment = '04891_range_i32';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0 AS triggered_i32
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_range_i32' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      )
      AND message LIKE '%Converted join hash map to fixed hash map%'
      AND message LIKE '%type: range%'
      AND message NOT LIKE '%two_level%';

SELECT '-- range int32 inner conversion on vs off';
SELECT
    (
        SELECT groupArray((k, lv, rv))
        FROM
        (
            SELECT l.k AS k, l.v AS lv, r.v AS rv
            FROM t_range_i32_l AS l INNER JOIN t_range_i32_r AS r ON l.k = r.k
            ORDER BY l.k, l.v, r.v
            SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 1
        )
    ) = (
        SELECT groupArray((k, lv, rv))
        FROM
        (
            SELECT l.k AS k, l.v AS lv, r.v AS rv
            FROM t_range_i32_l AS l INNER JOIN t_range_i32_r AS r ON l.k = r.k
            ORDER BY l.k, l.v, r.v
            SETTINGS parallel_hash_join_threshold = 0, enable_join_fixed_hash_table_conversion = 0
        )
    );

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

SELECT '-- hash table layouts';
SELECT 'key8_serial', count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Join hash table type: key8%' AND message NOT LIKE '%two_level_key8%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_key8_serial' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );
SELECT 'key8_parallel', count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Join hash table type: two_level_key8%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_key8_parallel' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );
SELECT 'key16_serial', count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Join hash table type: key16%' AND message NOT LIKE '%two_level_key16%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_key16_serial' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );
SELECT 'key16_parallel', count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Join hash table type: two_level_key16%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_key16_parallel' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );

SELECT '-- shared rf published';
SELECT 'serial_on', count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Published shared fixed-hash-table runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_rf_serial_on' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );
SELECT 'parallel_on', count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Published shared fixed-hash-table runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_rf_parallel_on' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );
SELECT 'serial_off', count() = 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Published shared fixed-hash-table runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_rf_serial_off' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );
SELECT 'parallel_off', count() = 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Published shared fixed-hash-table runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04891_rf_parallel_off' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      );

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
