SET max_threads = 4, max_block_size = 128, max_parallel_replicas = 1;
SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;
SET read_in_order_use_buffering = 1, read_in_order_virtual_row_prefetch_window = 0;
SET read_in_order_two_level_merge_threshold = 1000000;
SET optimize_move_to_prewhere = 0, use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0, use_skip_indexes = 0, log_queries = 1;

CREATE TABLE vrow_active (k UInt64, probe UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES vrow_active;
INSERT INTO vrow_active SELECT number, number FROM numbers(32768);
INSERT INTO vrow_active SELECT number + 32768, number FROM numbers(32768);
INSERT INTO vrow_active SELECT number + 65536, number FROM numbers(32768);
INSERT INTO vrow_active SELECT number + 98304, number FROM numbers(32768);

-- An active stream can buffer with a zero speculative window. A large answer
-- spans many small chunks and must leave the later disjoint streams unread.
SELECT groupArray(k) = range(0, 10000, 2)
FROM (SELECT k FROM vrow_active WHERE probe % 2 = 0 ORDER BY k LIMIT 5000)
SETTINGS read_in_order_use_virtual_row_per_block = 0, log_comment = '05137_initial',
         prefer_external_sort_block_bytes = 65536;

-- Per-block announcements must preserve every chunk in the active queue.
SELECT groupArray(k) = range(0, 10000, 2)
FROM (SELECT k FROM vrow_active WHERE probe % 2 = 0 ORDER BY k LIMIT 5000)
SETTINGS read_in_order_use_virtual_row_per_block = 1, log_comment = '05137_per_block',
         prefer_external_sort_block_bytes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT count() = 2 AND max(read_rows) <= 10240
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05137_initial', '05137_per_block') AND event_date >= today() - 1;

-- Exhaust the first stream, then drain queued rows from later streams. Each
-- stream may stop after its own limit, while the merge applies the global limit.
SELECT groupArray(k) = arraySlice(range(0, 131072, 16), 1, 5000)
FROM (SELECT k FROM vrow_active WHERE probe % 16 = 0 ORDER BY k LIMIT 5000)
SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT groupArray(k) = arraySlice(range(0, 131072, 16), 1, 5000)
FROM (SELECT k FROM vrow_active WHERE probe % 16 = 0 ORDER BY k LIMIT 5000)
SETTINGS read_in_order_use_virtual_row_per_block = 1, read_in_order_virtual_row_prefetch_window = 2;

-- Removing the limit must drain the complete contents of all stream queues.
SELECT groupArray(k) = range(0, 131072, 16)
FROM (SELECT k FROM vrow_active WHERE probe % 16 = 0 ORDER BY k)
SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT groupArray(k) = range(0, 131072, 16)
FROM (SELECT k FROM vrow_active WHERE probe % 16 = 0 ORDER BY k)
SETTINGS read_in_order_use_virtual_row_per_block = 1, read_in_order_use_buffering = 0,
         read_in_order_virtual_row_prefetch_window = 2;

DROP TABLE vrow_active;
