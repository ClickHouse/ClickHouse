-- `ReadFromMergeTree` shrinks an unnecessarily large number of streams down to the amount of data,
-- but the check used to compute `num_streams * min_marks_for_concurrent_read`, which overflows for
-- huge stream counts: the clamp was then skipped and the reading code went on to allocate per-stream
-- structures for an absurd number of streams. Here `min_marks_for_concurrent_read` is pinned to 16 and
-- `max_threads * max_streams_to_max_threads_ratio` to 2^60, so the product is exactly 2^64 and wraps to
-- zero. The stream count is representable and passes the planner's bounds check. A direct
-- `MergeTree` read reaches `ReadFromMergeTree` unchanged; a `Merge` read is rejected first.
DROP TABLE IF EXISTS t_marks_overflow;
CREATE TABLE t_marks_overflow (id Int32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_marks_overflow SELECT number FROM numbers(10);

SET max_threads = 4;
SET max_streams_to_max_threads_ratio = 288230376151711744;
SET merge_tree_min_read_task_size = 8;
SET merge_tree_min_rows_for_concurrent_read = 131072;
SET merge_tree_min_bytes_for_concurrent_read = 0;
SET merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 131072;
SET merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 0;

-- The in-order path (`spreadMarkRangesAmongStreamsWithOrder`).
SELECT id FROM merge(currentDatabase(), '^t_marks_overflow$') ORDER BY id DESC SETTINGS optimize_read_in_order = 1; -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT id FROM t_marks_overflow ORDER BY id DESC SETTINGS optimize_read_in_order = 1;

-- The default path (`spreadMarkRangesAmongStreams`).
SELECT count() FROM merge(currentDatabase(), '^t_marks_overflow$') SETTINGS optimize_read_in_order = 0; -- { serverError PARAMETER_OUT_OF_BOUND }
SELECT count() FROM t_marks_overflow SETTINGS optimize_read_in_order = 0;

DROP TABLE t_marks_overflow;
