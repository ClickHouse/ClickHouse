-- Reading a range in reverse order buffers the whole read task before emitting any of it, so a
-- query that stops early leaves part of what it read in that buffer. Those rows must still be
-- reported, otherwise `SelectedRows` / `read_rows` / quotas / `max_rows_to_read` do not see them.

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

DROP TABLE IF EXISTS t_reverse_read_progress;

-- Wide parts are required: a Compact part returns the whole range in one read, so nothing is
-- left buffered. `max_block_size` is deliberately not a multiple of `index_granularity`, which
-- makes a full-size range span two reads.
CREATE TABLE t_reverse_read_progress (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

INSERT INTO t_reverse_read_progress SELECT number FROM numbers(100);

SELECT a FROM t_reverse_read_progress ORDER BY a DESC LIMIT 13 FORMAT Null
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_block_size = 10,
         preferred_block_size_bytes = 0, use_query_condition_cache = 0,
         read_in_order_use_virtual_row = 0, query_plan_optimize_lazy_materialization = 0,
         use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0,
         enable_parallel_replicas = 0,
         log_comment = 'reverse_read_in_order_buffered_progress';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SelectedRows'] >= ProfileEvents['RowsReadByMainReader']
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND log_comment = 'reverse_read_in_order_buffered_progress';

-- The rows left in the buffer count towards `max_rows_to_read`. This also guards the assertion
-- above against becoming vacuous: if the query stopped leaving nothing buffered, the reported
-- total would stay below the limit and this statement would not throw.
SELECT a FROM t_reverse_read_progress ORDER BY a DESC LIMIT 13 FORMAT Null
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_block_size = 10,
         preferred_block_size_bytes = 0, use_query_condition_cache = 0,
         read_in_order_use_virtual_row = 0, query_plan_optimize_lazy_materialization = 0,
         use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0,
         enable_parallel_replicas = 0,
         max_rows_to_read = 20; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_reverse_read_progress;
