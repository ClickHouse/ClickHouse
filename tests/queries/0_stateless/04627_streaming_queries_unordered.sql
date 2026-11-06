-- Tags: no-parallel-replicas
-- UNORDERED streaming does not sort each snapshot by cursor (no in-memory sort, works without a commit-order projection); ordering holds only between snapshots.

SET enable_analyzer = 1; -- streaming queries require the analyzer (CI randomizes this setting)
SET enable_streaming_queries = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_streaming_unordered;

CREATE TABLE t_streaming_unordered (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_streaming_unordered SELECT number, number * 10 FROM numbers(5);
INSERT INTO t_streaming_unordered SELECT number, number * 10 FROM numbers(5, 5);

-- BOUNDED UNORDERED reads the same set of rows as BOUNDED, just not sorted within the snapshot.
-- Order-independent aggregates make the comparison robust.
SELECT 'ordered', count(), sum(k), sum(v) FROM t_streaming_unordered STREAM BOUNDED;
SELECT 'unordered', count(), sum(k), sum(v) FROM t_streaming_unordered STREAM BOUNDED UNORDERED;

DROP TABLE t_streaming_unordered;
