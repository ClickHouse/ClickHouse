-- Tags: no-parallel-replicas
-- The sliced pool is used for local reading only.

-- The sliced pool reads ahead of the merge in the order of the primary key: the next slices to read
-- are the ones with the smallest key at their first mark, whichever part they belong to. While the
-- answer lies in one part, the read-ahead stays in that part, and parts whose keys come later are
-- not touched at all.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 100;
SET use_query_condition_cache = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
-- Slices of at most 4 marks, so that a part is read as many slices.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS t_sliced_ahead;

CREATE TABLE t_sliced_ahead (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_sliced_ahead;

-- Four parts with disjoint key ranges of 20000 keys each.
INSERT INTO t_sliced_ahead SELECT number, number * 7 FROM numbers(0, 20000);
INSERT INTO t_sliced_ahead SELECT number, number * 7 FROM numbers(20000, 20000);
INSERT INTO t_sliced_ahead SELECT number, number * 7 FROM numbers(40000, 20000);
INSERT INTO t_sliced_ahead SELECT number, number * 7 FROM numbers(60000, 20000);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_ahead' AND active;

-- Without a filter nothing is read ahead: one slice of one granule answers the query.
SELECT 'dense', k FROM t_sliced_ahead ORDER BY k LIMIT 3 SETTINGS log_comment = '05293_dense';

-- One row in 200 matches (v = 7 * k), so every slice comes back mostly filtered out and the pool reads
-- ahead with all threads. The first 50 matches lie in the first 10000 rows of the first part: the
-- read-ahead may run a few slices further, but it must not leave the first part.
SELECT 'first part', count(), min(k), max(k) FROM (SELECT k FROM t_sliced_ahead PREWHERE v % 1400 = 0 ORDER BY k LIMIT 50)
SETTINGS log_comment = '05293_first_part';

-- The first 120 matches reach into the second part; the third and fourth part stay untouched.
SELECT 'two parts', count(), min(k), max(k) FROM (SELECT k FROM t_sliced_ahead PREWHERE v % 1400 = 0 ORDER BY k LIMIT 120)
SETTINGS log_comment = '05293_two_parts';

-- The same rows as with one reading thread per part, with the filter before and after reading.
SELECT 'prewhere', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced_ahead PREWHERE v % 1400 = 0 ORDER BY k LIMIT 300);
SELECT 'prewhere', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced_ahead PREWHERE v % 1400 = 0 ORDER BY k LIMIT 300) SETTINGS read_in_order_use_sliced_pool = 0;
SELECT 'where', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced_ahead WHERE v % 1400 = 0 ORDER BY k LIMIT 300) SETTINGS optimize_move_to_prewhere = 0;
SELECT 'where', cityHash64(groupArray(k)) FROM (SELECT k FROM t_sliced_ahead WHERE v % 1400 = 0 ORDER BY k LIMIT 300) SETTINGS optimize_move_to_prewhere = 0, read_in_order_use_sliced_pool = 0;

SYSTEM FLUSH LOGS query_log;

-- Rows read: one granule for the dense query, the first part (20000 rows) at most for the first 50
-- matches, the first two parts at most for the first 120.
SELECT log_comment, read_rows <= multiIf(log_comment = '05293_dense', 128, log_comment = '05293_first_part', 20000, 40000) AS within_bound
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05293_dense', '05293_first_part', '05293_two_parts')
ORDER BY log_comment;

DROP TABLE t_sliced_ahead;
