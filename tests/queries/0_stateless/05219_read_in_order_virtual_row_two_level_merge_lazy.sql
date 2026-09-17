-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- no-parallel-replicas: the test asserts read counts in query_log for a single-node
--                       in-order merge.

-- A preliminary merge of a two-level in-order merge announces the key its output starts at
-- before it reads any of its parts, so the final merge defers whole groups the way it
-- defers single parts: a limit answered at the start of the key range must not read a block
-- from every group.

DROP TABLE IF EXISTS t_two_level_lazy;

CREATE TABLE t_two_level_lazy (x UInt64, v UInt64) ENGINE = MergeTree ORDER BY x;

SYSTEM STOP MERGES t_two_level_lazy;

-- 32 parts of 100000 rows with disjoint key ranges in key order.
INSERT INTO t_two_level_lazy SELECT number, number FROM numbers(3200000)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_two_level_lazy' AND active;

-- The read goes through preliminary merges below the final one.
SELECT count() > 1 FROM (EXPLAIN PIPELINE SELECT x FROM t_two_level_lazy ORDER BY x LIMIT 3
    SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 16, read_in_order_use_virtual_row = 1)
WHERE explain LIKE '%MergingSortedTransform%';

-- Force two-level merging with 16 groups of two parts.
SELECT x FROM t_two_level_lazy ORDER BY x LIMIT 3
SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 16,
         read_in_order_use_virtual_row = 1, use_query_condition_cache = 0,
         log_comment = '05219_two_level_lazy';

SELECT x FROM t_two_level_lazy ORDER BY x LIMIT 3
SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 16,
         read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1, read_in_order_virtual_row_block_interval = 1,
         use_query_condition_cache = 0,
         log_comment = '05219_two_level_lazy_per_block';

SYSTEM FLUSH LOGS system.query_log;

-- Only the front group is read; the deferred groups add nothing. Without the deferral every
-- group reads a block from its front part (16 blocks of 8192 rows here).
SELECT log_comment, if(read_rows <= 8 * 8192, 'Ok', format('Too many rows read: {}', read_rows))
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '05219_two_level_lazy%'
    AND type = 'QueryFinish' AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_two_level_lazy;
