-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- ^ no-parallel-replicas because we use query_log

SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_virtual_row_per_block = 1;
SET optimize_read_in_order = 1;
SET use_query_condition_cache = 0;
SET merge_tree_min_read_task_size = 1024;

DROP TABLE IF EXISTS t_vrow_1;

CREATE TABLE t_vrow_1 (event_time DateTime, filter_val UInt64, data String)
ENGINE = MergeTree ORDER BY event_time
PARTITION BY modulo(sipHash64(data), 256)
SETTINGS index_granularity = 8192,
    add_minmax_index_for_numeric_columns = 0,
    index_granularity_bytes = 1024,
    min_index_granularity_bytes = 1024;

--- Write overlapping data to multiple parts, only one part matches the filter
INSERT INTO t_vrow_1 SELECT toDateTime('2026-01-01 13:00:00') + number, 1, 'x' FROM numbers(100_000);

-- overlap less than one granule
INSERT INTO t_vrow_1 SELECT (SELECT min(event_time) FROM t_vrow_1 WHERE filter_val = 1) -  1000 + number, 0, 'a' FROM numbers(100_000);
INSERT INTO t_vrow_1 SELECT (SELECT max(event_time) FROM t_vrow_1 WHERE filter_val = 1) +  1000 - number, 0, 'c' FROM numbers(100_000);

-- overlap three granules
INSERT INTO t_vrow_1 SELECT (SELECT min(event_time) FROM t_vrow_1 WHERE filter_val = 1) - 17000 + number, 0, 'b' FROM numbers(100_000);
INSERT INTO t_vrow_1 SELECT (SELECT max(event_time) FROM t_vrow_1 WHERE filter_val = 1) + 17000 - number, 0, 'd' FROM numbers(100_000);

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SET max_block_size = 8192;
SET read_in_order_two_level_merge_threshold = DEFAULT;

SELECT * FROM t_vrow_1
WHERE filter_val = 1
ORDER BY event_time ASC
LIMIT 10
FORMAT Null
SETTINGS log_comment = 'vrow_filter_1';

SELECT * FROM t_vrow_1
WHERE filter_val = 1
ORDER BY event_time DESC
LIMIT 10
FORMAT Null
SETTINGS log_comment = 'vrow_filter_reverse_1';

-- PREWHERE fully filters out all blocks from 3 of 4 parts (same filter, different code path).
SELECT * FROM t_vrow_1
PREWHERE filter_val = 1
ORDER BY event_time ASC
LIMIT 10
FORMAT Null
SETTINGS log_comment = 'vrow_prewhere_1';

SELECT * FROM t_vrow_1
PREWHERE filter_val = 1
ORDER BY event_time DESC
LIMIT 10
FORMAT Null
SETTINGS log_comment = 'vrow_prewhere_reverse_1';

SYSTEM FLUSH LOGS system.query_log;

SELECT log_comment, if(read_rows <= 8192 * 8, 'Ok', format('Too many rows read: {}, query_id: {}', read_rows, query_id)) AS result
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= (SELECT ts FROM start_ts)
    AND current_database = currentDatabase()
    AND log_comment LIKE 'vrow_%'
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
ORDER BY log_comment;
