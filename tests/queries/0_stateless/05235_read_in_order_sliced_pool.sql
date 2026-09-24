-- Tags: no-parallel-replicas
-- The sliced pool is used for local reading only.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
-- Slices of 8 marks, so that a part is read as several slices and segments.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;

DROP TABLE IF EXISTS t_sliced;

CREATE TABLE t_sliced (k UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_sliced;

-- Four parts with overlapping key ranges.
INSERT INTO t_sliced SELECT number, number * 7, toString(number) FROM numbers(0, 20000);
INSERT INTO t_sliced SELECT number, number * 7, toString(number) FROM numbers(12000, 20000);
INSERT INTO t_sliced SELECT number, number * 7, toString(number) FROM numbers(16000, 20000);
INSERT INTO t_sliced SELECT number, number * 7, toString(number) FROM numbers(30000, 20000);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced' AND active;

SELECT 'router in pipeline', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%') > 0
FROM (EXPLAIN PIPELINE SELECT k FROM t_sliced ORDER BY k LIMIT 10 SETTINGS max_threads = 4);

SELECT 'no filter, 1 thread';
SELECT k FROM t_sliced ORDER BY k LIMIT 5 SETTINGS max_threads = 1;
SELECT 'no filter, 4 threads';
SELECT k FROM t_sliced ORDER BY k LIMIT 5 SETTINGS max_threads = 4;
SELECT 'where';
SELECT k, v FROM t_sliced WHERE v % 1000 = 0 ORDER BY k LIMIT 8 SETTINGS max_threads = 4;
SELECT 'prewhere';
SELECT k, v FROM t_sliced PREWHERE v % 1000 = 0 ORDER BY k LIMIT 8 SETTINGS max_threads = 8;

-- The same results as with one reading thread per part.
SELECT 'big limit', sum(k), count() FROM (SELECT k FROM t_sliced ORDER BY k LIMIT 30000) SETTINGS max_threads = 8;
SELECT 'big limit', sum(k), count() FROM (SELECT k FROM t_sliced ORDER BY k LIMIT 30000) SETTINGS max_threads = 8, read_in_order_use_sliced_pool = 0;
SELECT 'selective', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced WHERE s LIKE '%7%' ORDER BY k LIMIT 5000) SETTINGS max_threads = 8;
SELECT 'selective', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced WHERE s LIKE '%7%' ORDER BY k LIMIT 5000) SETTINGS max_threads = 8, read_in_order_use_sliced_pool = 0;
SELECT 'rare', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced WHERE v % 999 = 0 ORDER BY k LIMIT 100) SETTINGS max_threads = 4;
SELECT 'rare', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced WHERE v % 999 = 0 ORDER BY k LIMIT 100) SETTINGS max_threads = 4, read_in_order_use_sliced_pool = 0;
SELECT 'per block virtual row', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced WHERE v % 999 = 0 ORDER BY k LIMIT 100) SETTINGS max_threads = 4, read_in_order_use_virtual_row_per_block = 1;
SELECT 'no match', count() FROM (SELECT k FROM t_sliced WHERE v = 1 ORDER BY k LIMIT 10) SETTINGS max_threads = 4;
SELECT 'whole table', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced ORDER BY k) SETTINGS max_threads = 8;
SELECT 'whole table', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced ORDER BY k) SETTINGS max_threads = 8, read_in_order_use_sliced_pool = 0;

-- The answer lies in the first granules of the first part: the other parts must not be read.
-- One thread, so that the sources cannot run ahead of the merge while the pipeline finishes.
SELECT 'lazy', k FROM t_sliced ORDER BY k LIMIT 3 SETTINGS max_threads = 1, max_rows_to_read = 4000;

-- Descending order keeps the per-part reading.
SELECT 'desc', k FROM t_sliced ORDER BY k DESC LIMIT 3 SETTINGS max_threads = 4;

DROP TABLE t_sliced;
