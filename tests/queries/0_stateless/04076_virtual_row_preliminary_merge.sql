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

-- 115 parts with high keys [100000..): should be skippable via virtual rows
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   0, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   1, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   2, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   3, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   4, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   5, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   6, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   7, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   8, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +   9, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  10, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  11, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  12, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  13, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  14, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  15, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  16, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  17, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  18, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  19, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  20, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  21, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  22, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  23, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  24, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  25, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  26, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  27, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  28, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  29, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  30, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  31, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  32, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  33, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  34, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  35, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  36, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  37, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  38, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  39, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  40, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  41, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  42, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  43, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  44, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  45, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  46, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  47, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  48, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  49, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  50, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  51, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  52, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  53, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  54, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  55, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  56, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  57, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  58, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  59, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  60, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  61, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  62, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  63, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  64, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  65, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  66, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  67, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  68, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  69, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  70, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  71, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  72, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  73, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  74, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  75, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  76, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  77, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  78, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  79, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  80, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  81, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  82, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  83, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  84, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  85, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  86, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  87, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  88, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  89, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  90, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  91, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  92, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  93, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  94, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  95, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  96, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  97, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  98, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 +  99, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 100, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 101, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 102, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 103, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 104, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 105, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 106, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 107, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 108, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 109, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 110, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 111, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 112, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 113, randomString(8) FROM numbers(1000);
INSERT INTO t_vrow_prelim SELECT 100000 + number * 115 + 114, randomString(8) FROM numbers(1000);

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SET max_block_size = 64;
SET max_threads = 4;

SELECT '--';

SELECT k FROM t_vrow_prelim ORDER BY k ASC LIMIT 20
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
         read_in_order_two_level_merge_threshold = 100,
         log_comment = 'vrow_prelim';

SELECT '--';

SELECT k FROM t_vrow_prelim ORDER BY k ASC LIMIT 20
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
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
