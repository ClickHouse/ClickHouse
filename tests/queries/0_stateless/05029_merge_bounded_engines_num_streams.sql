-- `Memory` and `Null` bound their actual source counts regardless of the requested number
-- of streams. A `Merge` read over such children must not reject an excessive stream request
-- with `PARAMETER_OUT_OF_BOUND`: the children report their bounds through `getMaxReadStreams`
-- and the read succeeds. `View` cannot report a tighter bound (its inner query is replanned
-- from the query context, ignoring the requested stream count), so an excessive request over
-- a `View` child is rejected, while a modest one succeeds.

DROP TABLE IF EXISTS t_merge_memory_num_streams;
DROP TABLE IF EXISTS t_memory_num_streams;
DROP TABLE IF EXISTS t_merge_null_num_streams;
DROP TABLE IF EXISTS t_null_num_streams;
DROP TABLE IF EXISTS t_merge_view_num_streams;
DROP TABLE IF EXISTS t_view_num_streams;
DROP TABLE IF EXISTS t_view_inner_num_streams;

CREATE TABLE t_memory_num_streams (n UInt64) ENGINE = Memory;
INSERT INTO t_memory_num_streams VALUES (1);
CREATE TABLE t_merge_memory_num_streams (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_memory_num_streams$');

SELECT count() FROM t_merge_memory_num_streams
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 65536;

CREATE TABLE t_null_num_streams (n UInt64) ENGINE = Null;
CREATE TABLE t_merge_null_num_streams (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_null_num_streams$');

SELECT count() FROM t_merge_null_num_streams
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 65536;

CREATE TABLE t_view_inner_num_streams (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_view_inner_num_streams VALUES (1);
CREATE VIEW t_view_num_streams AS SELECT n FROM t_view_inner_num_streams;
CREATE TABLE t_merge_view_num_streams (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_view_num_streams$');

SELECT count() FROM t_merge_view_num_streams
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 65536; -- { serverError PARAMETER_OUT_OF_BOUND }

SELECT count() FROM t_merge_view_num_streams
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 4096;

DROP TABLE t_merge_memory_num_streams;
DROP TABLE t_memory_num_streams;
DROP TABLE t_merge_null_num_streams;
DROP TABLE t_null_num_streams;
DROP TABLE t_merge_view_num_streams;
DROP TABLE t_view_num_streams;
DROP TABLE t_view_inner_num_streams;
