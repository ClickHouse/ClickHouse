-- `max_threads * max_streams_to_max_threads_ratio` = 2^32 used to be truncated to 0 streams
-- by a `UInt32` cast in `StorageMerge`, and the child `ReadFromMergeTree` divided by it.
-- It is now rejected by the `Merge` stream-count limit before child plans are created.
DROP TABLE IF EXISTS t_merge_streams;
CREATE TABLE t_merge_streams (id Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_streams SELECT number FROM numbers(10);

SELECT id FROM merge(currentDatabase(), '^t_merge_streams$') ORDER BY id DESC
SETTINGS max_streams_to_max_threads_ratio = 1073741824, max_threads = 4, optimize_read_in_order = 1; -- { serverError PARAMETER_OUT_OF_BOUND }

DROP TABLE t_merge_streams;
