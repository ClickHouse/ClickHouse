-- RIGHT and FULL joins with an extra condition in ON must return the same rows with the serial
-- and the parallel layout. `parallel_hash_join_threshold = 1000000000` builds the serial layout and
-- `0` the parallel one. With the parallel layout, `UInt32` keys use a two-level hash map, `UInt16`
-- keys use a fixed-size map split into buckets, and several streams emit the unmatched right rows.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key UInt32, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (key UInt32, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 SELECT number, number * 4, concat('l', toString(number)) FROM numbers(6);
INSERT INTO t2 SELECT number, 10, concat('r', toString(number)) FROM numbers(12);

SET query_plan_join_swap_table = 0;
-- The tables are ordered by the join key, and a join sharded by key ranges always builds the serial layout.
SET query_plan_join_shard_by_pk_ranges = 0;
SET query_plan_optimize_join_order_limit = 10;
SET max_threads = 16;
SET max_block_size = 2;
SET parallel_non_joined_rows_processing = 1;

SELECT '---- RIGHT ANTI HASH';
SELECT t2.* FROM t1 RIGHT ANTI JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04508_right_anti_serial';
SELECT '---- RIGHT ANTI PARALLEL HASH';
SELECT t2.* FROM t1 RIGHT ANTI JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 0, log_comment = '04508_right_anti_parallel';

SELECT '---- RIGHT HASH';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04508_right_serial';
SELECT '---- RIGHT PARALLEL HASH';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 0, log_comment = '04508_right_parallel';

SELECT '---- FULL HASH';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04508_full_serial';
SELECT '---- FULL PARALLEL HASH';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.key = t2.key AND t1.a < t2.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 0, log_comment = '04508_full_parallel';

DROP TABLE IF EXISTS t1_16;
DROP TABLE IF EXISTS t2_16;

CREATE TABLE t1_16 (key UInt16, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_16 (key UInt16, a UInt32, attr String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1_16 SELECT number, number * 4, concat('l', toString(number)) FROM numbers(6);
INSERT INTO t2_16 SELECT number, 10, concat('r', toString(number)) FROM numbers(12);

SELECT '---- UINT16 RIGHT ANTI HASH';
SELECT t2_16.* FROM t1_16 RIGHT ANTI JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04508_uint16_right_anti_serial';
SELECT '---- UINT16 RIGHT ANTI PARALLEL HASH';
SELECT t2_16.* FROM t1_16 RIGHT ANTI JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 0, log_comment = '04508_uint16_right_anti_parallel';

SELECT '---- UINT16 RIGHT HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 RIGHT JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04508_uint16_right_serial';
SELECT '---- UINT16 RIGHT PARALLEL HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 RIGHT JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 0, log_comment = '04508_uint16_right_parallel';

SELECT '---- UINT16 FULL HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 FULL JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '04508_uint16_full_serial';
SELECT '---- UINT16 FULL PARALLEL HASH';
SELECT t1_16.*, t2_16.* FROM t1_16 FULL JOIN t2_16 ON t1_16.key = t2_16.key AND t1_16.a < t2_16.a ORDER BY ALL SETTINGS parallel_hash_join_threshold = 0, log_comment = '04508_uint16_full_parallel';

-- Each serial query must have built the serial layout, and each parallel query the parallel one.
-- Read the log in default-size blocks: with two-row blocks this read takes minutes under the thread fuzzer.
SET max_block_size = DEFAULT;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('04508_right_anti_serial', '04508_right_anti_parallel', '04508_right_serial', '04508_right_parallel',
        '04508_full_serial', '04508_full_parallel', '04508_uint16_right_anti_serial', '04508_uint16_right_anti_parallel',
        '04508_uint16_right_serial', '04508_uint16_right_parallel', '04508_uint16_full_serial', '04508_uint16_full_parallel')
ORDER BY event_time_microseconds;

DROP TABLE t1_16;
DROP TABLE t2_16;

DROP TABLE t1;
DROP TABLE t2;
