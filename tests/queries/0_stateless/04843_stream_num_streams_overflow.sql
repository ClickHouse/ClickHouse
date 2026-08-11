-- Tags: no-parallel-replicas
-- A streaming read (`FROM ... STREAM`) has no marks by which the number of streams could be
-- clamped, and `groupPartitionsByStreams` creates one `MergeTreeCommitOrderSequentialSource`
-- per stream. Before the fix, a huge `max_threads * max_streams_to_max_threads_ratio` product
-- made it try to allocate that many sources: `pipes.reserve` threw `std::length_error` (a logical
-- error in debug builds) for astronomic values, and merely absurd values exhausted memory.
-- Now such values are rejected with `PARAMETER_OUT_OF_BOUND`.

SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_stream_num_streams;

CREATE TABLE t_stream_num_streams (n UInt64)
ENGINE = MergeTree ORDER BY n
SETTINGS enable_block_number_column = 1,
         enable_block_offset_column = 1,
         add_minmax_index_for_block_number_column = 1,
         add_minmax_index_for_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_stream_num_streams VALUES (1);

-- 4 * 2^58 = 2^60 streams. `max_streams_for_merge_tree_reading` is pinned to 0 because a nonzero
-- value would clamp the stream count in the `ReadFromMergeTree` constructor before it reaches
-- the streaming path.
SELECT n FROM t_stream_num_streams STREAM
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 288230376151711744, max_streams_for_merge_tree_reading = 0
FORMAT Null; -- { serverError PARAMETER_OUT_OF_BOUND }

-- A sane stream count still works.
SELECT count() FROM (SELECT n FROM t_stream_num_streams STREAM LIMIT 1)
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 2, max_streams_for_merge_tree_reading = 0;

DROP TABLE t_stream_num_streams;
