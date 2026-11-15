-- Emitting a virtual row only after every N-th block must not change the results
-- and must keep the part-start virtual rows that let a LIMIT stop early.
SET max_threads = 4, max_block_size = 128, max_parallel_replicas = 1;
SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1;
SET read_in_order_use_buffering = 1, read_in_order_two_level_merge_threshold = 1000000;
SET optimize_move_to_prewhere = 0, use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0, use_skip_indexes = 0, log_queries = 1;

CREATE TABLE vrow_interval (k UInt64, probe UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES vrow_interval;
INSERT INTO vrow_interval SELECT number, number FROM numbers(16384);
INSERT INTO vrow_interval SELECT number + 16384, number + 16384 FROM numbers(16384);
INSERT INTO vrow_interval SELECT number + 32768, number + 32768 FROM numbers(16384);
INSERT INTO vrow_interval SELECT number + 49152, number + 49152 FROM numbers(16384);

-- Sparse matches spread over all parts, forward and reverse order.
SELECT groupArray(k) = arraySlice(range(0, 65536, 1000), 1, 40)
FROM (SELECT k FROM vrow_interval WHERE probe % 1000 = 0 ORDER BY k LIMIT 40)
SETTINGS read_in_order_virtual_row_block_interval = 1;

SELECT groupArray(k) = arraySlice(range(0, 65536, 1000), 1, 40)
FROM (SELECT k FROM vrow_interval WHERE probe % 1000 = 0 ORDER BY k LIMIT 40)
SETTINGS read_in_order_virtual_row_block_interval = 2;

SELECT groupArray(k) = arraySlice(range(0, 65536, 1000), 1, 40)
FROM (SELECT k FROM vrow_interval WHERE probe % 1000 = 0 ORDER BY k LIMIT 40)
SETTINGS read_in_order_virtual_row_block_interval = 7;

SELECT groupArray(k) = arraySlice(range(0, 65536, 1000), 1, 40)
FROM (SELECT k FROM vrow_interval WHERE probe % 1000 = 0 ORDER BY k LIMIT 40)
SETTINGS read_in_order_virtual_row_block_interval = 1000000;

SELECT groupArray(k) = arraySlice(arrayReverse(range(0, 65536, 1000)), 1, 40)
FROM (SELECT k FROM vrow_interval WHERE probe % 1000 = 0 ORDER BY k DESC LIMIT 40)
SETTINGS read_in_order_virtual_row_block_interval = 3;

SELECT groupArray(k) = arraySlice(arrayReverse(range(0, 65536, 1000)), 1, 40)
FROM (SELECT k FROM vrow_interval WHERE probe % 1000 = 0 ORDER BY k DESC LIMIT 40)
SETTINGS read_in_order_virtual_row_block_interval = 1000000;

-- A filter that survives only in the last part, with and without a limit.
SELECT groupArray(k) = range(49152, 49152 + 16384, 16)
FROM (SELECT k FROM vrow_interval WHERE probe % 16 = 0 AND k >= 49152 ORDER BY k)
SETTINGS read_in_order_virtual_row_block_interval = 5;

SELECT groupArray(k) = arraySlice(range(49152, 49152 + 16384, 16), 1, 100)
FROM (SELECT k FROM vrow_interval WHERE probe % 16 = 0 AND k >= 49152 ORDER BY k LIMIT 100)
SETTINGS read_in_order_virtual_row_block_interval = 5;

-- The answer lies in the first part; the later parts must stay unread for any interval.
SELECT groupArray(k) = range(0, 2000, 2)
FROM (SELECT k FROM vrow_interval WHERE probe % 2 = 0 ORDER BY k LIMIT 1000)
SETTINGS read_in_order_virtual_row_block_interval = 1, log_comment = '05212_interval_1';

SELECT groupArray(k) = range(0, 2000, 2)
FROM (SELECT k FROM vrow_interval WHERE probe % 2 = 0 ORDER BY k LIMIT 1000)
SETTINGS read_in_order_virtual_row_block_interval = 4, log_comment = '05212_interval_4';

SELECT groupArray(k) = range(0, 2000, 2)
FROM (SELECT k FROM vrow_interval WHERE probe % 2 = 0 ORDER BY k LIMIT 1000)
SETTINGS read_in_order_virtual_row_block_interval = 1000000, log_comment = '05212_interval_max';

SYSTEM FLUSH LOGS query_log;
SELECT count() = 3 AND max(read_rows) <= 8192
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05212_interval_1', '05212_interval_4', '05212_interval_max') AND event_date >= today() - 1;

DROP TABLE vrow_interval;
