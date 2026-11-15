-- Tags: no-parallel-replicas, no-object-storage
-- The sliced pool is used for local reading only; the bounds below assume the slice sizes of parts
-- on local disks.

-- Whenever the merge asks for a lane and no rows are ready, the router announces the primary key the
-- lane's next rows start at, so the merge steps past a part whose rows are all filtered out as soon
-- as the announced key is past the rows it needs, instead of reading the part to its end.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 100;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
SET max_insert_threads = 1;
-- Slices of at most 4 marks of 128 rows: the ramp of a lane is 1 and 2 marks, and the read-ahead
-- budget after the ramp is one slice per source.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS t_sliced_announce;
DROP TABLE IF EXISTS t_sliced_interleaved;

-- A part of 65536 rows where no row passes the filter, and a one-row part with key 1000, which lies
-- in the eighth granule of the big part.
CREATE TABLE t_sliced_announce (x UInt64, p UInt8, v UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_sliced_announce;
INSERT INTO t_sliced_announce SELECT number, 0, 0 FROM numbers(65536);
INSERT INTO t_sliced_announce VALUES (1000, 1, 1);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_announce' AND active;

-- The big part is read slice by slice until its announced key passes 1000, then the merge takes the one row.
SELECT 'one row behind an empty part', x FROM t_sliced_announce WHERE v = 1 ORDER BY x LIMIT 1
SETTINGS max_threads = 1, log_comment = '05315_announce';

-- Eight parts whose keys interleave (part i holds the keys equal to i modulo 8) and one row that
-- passes the filter. The merge needs every part up to that key and must step past the seven parts
-- that yield nothing; with their keys announced at every slice it does so after a few slices of each.
CREATE TABLE t_sliced_interleaved (x UInt64, v UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_sliced_interleaved;
INSERT INTO t_sliced_interleaved SELECT number * 8 + 0, number * 8 + 0 = 4000 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 1, 0 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 2, 0 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 3, 0 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 4, 0 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 5, 0 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 6, 0 FROM numbers(8192);
INSERT INTO t_sliced_interleaved SELECT number * 8 + 7, 0 FROM numbers(8192);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_interleaved' AND active;

-- One source, so that nothing is read ahead while the merge steps from lane to lane.
SELECT 'one row among interleaved parts', x FROM t_sliced_interleaved WHERE v = 1 ORDER BY x LIMIT 1
SETTINGS max_threads = 1, log_comment = '05315_interleaved';
SELECT 'one row among interleaved parts', x FROM t_sliced_interleaved WHERE v = 1 ORDER BY x LIMIT 1;
-- The same rows as with one reading thread per part.
SELECT 'first rows', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_interleaved WHERE x % 3 = 0 ORDER BY x LIMIT 1000);
SELECT 'first rows', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_interleaved WHERE x % 3 = 0 ORDER BY x LIMIT 1000) SETTINGS read_in_order_use_sliced_pool = 0;

SYSTEM FLUSH LOGS query_log;

-- Rows read, against the 65536 rows that yield nothing in every case (the per-part reading reads
-- them all for both queries). The full slice is 8 marks (`merge_tree_min_read_task_size`):
--   announce: the slices of the big part until its key passes 1000 (1, 2, 4 and 8 marks reach key
--     1920) and the one row: 1921;
--   interleaved: 7 marks of every part, the ramp and one full slice, whose announced key 7168 is
--     past the target: 7168.
SELECT replaceOne(log_comment, '05315_', '') AS query,
    read_rows <= if(query = 'announce', 1921, 7168) AS within_bound,
    result_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05315_announce', '05315_interleaved')
ORDER BY query, event_time_microseconds DESC
LIMIT 1 BY query;

DROP TABLE t_sliced_announce;
DROP TABLE t_sliced_interleaved;
