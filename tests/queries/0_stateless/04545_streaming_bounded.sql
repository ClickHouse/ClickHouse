-- Tags: no-parallel-replicas
-- Bounded streaming reads the first snapshot (everything committed so far) and then
-- finishes, instead of subscribing for updates like a plain STREAM query. Because it
-- terminates, it can be exercised with a plain synchronous query.

SET enable_analyzer = 1; -- streaming queries require the analyzer (CI randomizes this setting)
SET enable_streaming_queries = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_streaming_bounded;

CREATE TABLE t_streaming_bounded (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

-- Empty table: a bounded stream must return nothing and terminate (not hang).
SELECT 'empty', count() FROM t_streaming_bounded STREAM BOUNDED;

INSERT INTO t_streaming_bounded SELECT number, number * 10 FROM numbers(5);
INSERT INTO t_streaming_bounded SELECT number, number * 10 FROM numbers(5, 5);

-- Bounded stream reads everything committed so far, then terminates. Aggregates are
-- order-independent, so this does not rely on within-snapshot row order.
SELECT 'all', count(), sum(k), sum(v) FROM t_streaming_bounded STREAM BOUNDED;

DROP TABLE t_streaming_bounded;
