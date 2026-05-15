DROP TABLE IF EXISTS t_group_concat_overload;
CREATE TABLE t_group_concat_overload (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Single-part insert, `max_threads = 1` and no parallel replicas pin the row order so the limit/delimiter outputs are stable.
SET max_insert_threads = 1, max_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, enable_parallel_replicas = 0, parallel_replicas_for_non_replicated_merge_tree = 0;
INSERT INTO t_group_concat_overload SELECT number FROM numbers(5);
OPTIMIZE TABLE t_group_concat_overload FINAL;

SELECT 'baseline:', groupConcat(',', 2)(x) FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'limit kept:', groupConcat(',', 2)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'delim overridden:', groupConcat(',', 3)(x, '|') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'large limit:', groupConcat(',', 100)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

DROP TABLE t_group_concat_overload;
