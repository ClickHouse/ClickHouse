-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- ^ no-parallel-replicas because we use query_log

SET optimize_read_in_order = 1;
SET use_query_condition_cache = 0;
SET merge_tree_min_read_task_size = 1024;

DROP TABLE IF EXISTS t_vrow_prelim;

CREATE TABLE t_vrow_prelim (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64;

SYSTEM STOP MERGES t_vrow_prelim;

-- 5 parts with low keys [0..5000): these contain the LIMIT target rows
INSERT INTO t_vrow_prelim SELECT number, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 1000 + number, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 2000 + number, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 3000 + number, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 4000 + number, randomString(8) FROM numbers(1000);

-- 15 parts with high keys [100000..): should be skippable via virtual rows
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  0, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  1, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  2, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  3, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  4, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  5, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  6, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  7, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  8, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 +  9, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 + 10, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 + 11, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 + 12, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 + 13, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 15 + 14, randomString(8) FROM numbers(1000);

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SET max_block_size = 64;
SET max_threads = 4;

SELECT '--';

SELECT k FROM t_vrow_prelim ORDER BY k ASC LIMIT 20
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1, read_in_order_virtual_row_block_interval = 1,
         read_in_order_two_level_merge_threshold = 10,
         log_comment = 'vrow_prelim';

SELECT '--';

SELECT k FROM t_vrow_prelim ORDER BY k ASC LIMIT 20
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1, read_in_order_virtual_row_block_interval = 1,
         read_in_order_two_level_merge_threshold = 10000,
         log_comment = 'vrow_single';

SYSTEM FLUSH LOGS system.query_log;

SELECT log_comment,
    if(read_rows <= 1000, 'Ok', format('Too many rows read: {}, query_id: {}', read_rows, query_id)) AS result
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= (SELECT ts FROM start_ts)
    AND current_database = currentDatabase()
    AND log_comment LIKE 'vrow_%'
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
ORDER BY log_comment;

DROP TABLE t_vrow_prelim;
