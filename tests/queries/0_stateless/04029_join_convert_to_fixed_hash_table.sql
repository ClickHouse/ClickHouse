-- Test on-the-fly conversion of hash table to fixed hash table in hash join.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right_i32;
DROP TABLE IF EXISTS t_right_i64;
DROP TABLE IF EXISTS t_right_neg;

CREATE TABLE t_left (id Int32, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_i32 (id Int32, rval String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_i64 (id Int64, rval String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right_neg (id Int32, rval String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left SELECT number - 5, 'l' || toString(number - 5) FROM numbers(10);
INSERT INTO t_right_i32 VALUES (0, 'r0'), (2, 'r2'), (4, 'r4');
INSERT INTO t_right_i64 VALUES (0, 'r0'), (2, 'r2'), (4, 'r4');
INSERT INTO t_right_neg VALUES (-2, 'r-2'), (0, 'r0'), (2, 'r2');

SET join_algorithm = 'hash';
SET enable_join_fixed_hash_table_conversion = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

-- Verify conversion for Int32
SELECT '-- trigger check Int32';
SELECT count(*) FROM t_left JOIN t_right_i32 ON t_left.id = t_right_i32.id FORMAT NULL SETTINGS log_comment = '04029_range_hash_trigger_i32';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0 AS triggered
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id IN ( SELECT query_id FROM system.query_log WHERE log_comment = '04029_range_hash_trigger_i32' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() )
      AND message LIKE '%Converted join hash map to fixed hash map%';

-- Verify conversion for Int64
SELECT '-- trigger check Int64';
SELECT count(*) FROM t_left JOIN t_right_i64 ON t_left.id = t_right_i64.id FORMAT NULL SETTINGS log_comment = '04029_range_hash_trigger_i64';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0 AS triggered
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id IN ( SELECT query_id FROM system.query_log WHERE log_comment = '04029_range_hash_trigger_i64' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() )
      AND message LIKE '%Converted join hash map to fixed hash map%';

-- Verify conversion for negative range
SELECT '-- trigger check negative';
SELECT count(*) FROM t_left JOIN t_right_neg ON t_left.id = t_right_neg.id FORMAT NULL SETTINGS log_comment = '04029_range_hash_trigger_neg';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0 AS triggered
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id IN ( SELECT query_id FROM system.query_log WHERE log_comment = '04029_range_hash_trigger_neg' AND current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() )
      AND message LIKE '%Converted join hash map to fixed hash map%';

-- Verify results
-- ALL INNER JOIN
SELECT '-- ALL INNER Int32';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

SELECT '-- ALL INNER Int32 - AUTO JOIN';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS join_algorithm = 'auto';

SELECT '-- ALL INNER Int32 - GRACE HASH JOIN';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS join_algorithm = 'grace_hash';

SELECT '-- ALL INNER Int64';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i64 ON t_left.id = t_right_i64.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_i64 ON t_left.id = t_right_i64.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

SELECT '-- ALL INNER negative';
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_neg ON t_left.id = t_right_neg.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ALL INNER JOIN t_right_neg ON t_left.id = t_right_neg.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- ALL LEFT JOIN
SELECT '-- ALL LEFT Int32';
SELECT t_left.id, val, rval FROM t_left ALL LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ALL LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- ALL RIGHT JOIN
SELECT '-- ALL RIGHT Int32';
SELECT t_right_i32.id, val, rval FROM t_left ALL RIGHT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_right_i32.id;
SELECT t_right_i32.id, val, rval FROM t_left ALL RIGHT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_right_i32.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- ALL FULL JOIN
SELECT '-- ALL FULL Int32';
SELECT t_left.id, val, rval FROM t_left ALL FULL JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ALL FULL JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- ANY INNER JOIN
SELECT '-- ANY INNER Int32';
SELECT t_left.id, val, rval FROM t_left ANY INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ANY INNER JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- ANY LEFT JOIN
SELECT '-- ANY LEFT Int32';
SELECT t_left.id, val, rval FROM t_left ANY LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val, rval FROM t_left ANY LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- SEMI LEFT JOIN
SELECT '-- SEMI LEFT Int32';
SELECT t_left.id, val FROM t_left SEMI LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val FROM t_left SEMI LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

-- ANTI LEFT JOIN
SELECT '-- ANTI LEFT Int32';
SELECT t_left.id, val FROM t_left ANTI LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id;
SELECT t_left.id, val FROM t_left ANTI LEFT JOIN t_right_i32 ON t_left.id = t_right_i32.id ORDER BY t_left.id SETTINGS enable_join_fixed_hash_table_conversion = 0;

DROP TABLE t_left;
DROP TABLE t_right_i32;
DROP TABLE t_right_i64;
DROP TABLE t_right_neg;
