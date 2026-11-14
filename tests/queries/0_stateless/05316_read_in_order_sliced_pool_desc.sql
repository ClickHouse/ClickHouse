-- Tags: no-parallel-replicas, no-object-storage
-- The sliced pool is used for local reading only; the bounds below assume the slice sizes of parts
-- on local disks.

-- Reading in reverse order through the sliced pool: the slices of a part are cut from its end, parts
-- are queued by the key at their last unread mark, and the key a part's next rows start at from the
-- end is announced to the merge, so the merge steps past a part whose rows are all filtered out as
-- soon as that key is below the rows it needs.

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
-- Slices of at most 4 marks of 128 rows: the ramp of a lane is 1 and 2 marks.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS t_sliced_desc;
DROP TABLE IF EXISTS t_sliced_desc_interleaved;

-- A part of 65536 rows where no row passes the filter, and a one-row part with key 64535, which lies
-- in the eighth granule from the end of the big part.
CREATE TABLE t_sliced_desc (x UInt64, p UInt8, v UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_sliced_desc;
INSERT INTO t_sliced_desc SELECT number, 0, 0 FROM numbers(65536);
INSERT INTO t_sliced_desc VALUES (64535, 1, 1);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_desc' AND active;

SELECT 'router in pipeline', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%') > 0
FROM (EXPLAIN PIPELINE SELECT x FROM t_sliced_desc ORDER BY x DESC LIMIT 1);

-- Without a filter the last granule of the big part answers the query.
SELECT 'no filter', x FROM t_sliced_desc ORDER BY x DESC LIMIT 3 SETTINGS log_comment = '05316_dense';

-- The big part is read from its end slice by slice until its announced key falls below 64535, then the merge takes the one row.
SELECT 'one row behind an empty part', x FROM t_sliced_desc WHERE v = 1 ORDER BY x DESC LIMIT 1
SETTINGS max_threads = 1, log_comment = '05316_announce';

-- Eight parts whose keys interleave (part i holds the keys equal to i modulo 8) and one row that passes the filter, 4000 keys before the end.
CREATE TABLE t_sliced_desc_interleaved (x UInt64, v UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_sliced_desc_interleaved;
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 0, number * 8 + 0 = 61536 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 1, 0 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 2, 0 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 3, 0 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 4, 0 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 5, 0 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 6, 0 FROM numbers(8192);
INSERT INTO t_sliced_desc_interleaved SELECT number * 8 + 7, 0 FROM numbers(8192);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_desc_interleaved' AND active;

-- One source, so that nothing is read ahead while the merge steps from lane to lane.
SELECT 'one row among interleaved parts', x FROM t_sliced_desc_interleaved WHERE v = 1 ORDER BY x DESC LIMIT 1
SETTINGS max_threads = 1, log_comment = '05316_interleaved';
SELECT 'one row among interleaved parts', x FROM t_sliced_desc_interleaved WHERE v = 1 ORDER BY x DESC LIMIT 1;

-- The same rows as with one reading thread per part.
SELECT 'last rows', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_desc_interleaved WHERE x % 3 = 0 ORDER BY x DESC LIMIT 1000);
SELECT 'last rows', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_desc_interleaved WHERE x % 3 = 0 ORDER BY x DESC LIMIT 1000) SETTINGS read_in_order_use_sliced_pool = 0;
SELECT 'whole table', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_desc_interleaved ORDER BY x DESC) SETTINGS max_threads = 8;
SELECT 'whole table', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_desc_interleaved ORDER BY x DESC) SETTINGS max_threads = 8, read_in_order_use_sliced_pool = 0;
SELECT 'prewhere', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_desc_interleaved PREWHERE x % 7 = 0 ORDER BY x DESC LIMIT 3000) SETTINGS max_threads = 8;
SELECT 'prewhere', cityHash64(groupArray(x)) FROM (SELECT x FROM t_sliced_desc_interleaved PREWHERE x % 7 = 0 ORDER BY x DESC LIMIT 3000) SETTINGS max_threads = 8, read_in_order_use_sliced_pool = 0;

SYSTEM FLUSH LOGS query_log;

-- Rows read, the mirror image of the ascending cases:
--   dense: the last granule of the big part;
--   announce: the slices of the big part from its end until its key falls below 64535 (1, 2, 4 and 8
--     marks reach key 63616) and the one row: 1921;
--   interleaved: 7 marks of every part from its end, whose announced key 58368 is below the target: 7168.
SELECT replaceOne(log_comment, '05316_', '') AS query,
    read_rows <= multiIf(query = 'dense', 128, query = 'announce', 1921, 7168) AS within_bound,
    result_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05316_dense', '05316_announce', '05316_interleaved')
ORDER BY query, event_time_microseconds DESC
LIMIT 1 BY query;

DROP TABLE t_sliced_desc;
DROP TABLE t_sliced_desc_interleaved;
