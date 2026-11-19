-- Tags: no-parallel-replicas
-- A bounded streaming read on a read-only table must terminate: it relies on the background streaming
-- assignee to deliver the first subscription update (the initial snapshot, or the proof that the table
-- is empty). A table that starts read-only, whether created or attached with `table_readonly = 1`,
-- does not run the workers that modify data, but it must still run this read-only one.

SET enable_analyzer = 1; -- streaming queries require the analyzer (CI randomizes this setting)
SET enable_streaming_queries = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_readonly_streaming;

-- Created read-only and empty: the bounded stream must return nothing and finish, not hang.
CREATE TABLE t_readonly_streaming (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    table_readonly = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

SELECT 'created read-only, empty', count() FROM t_readonly_streaming STREAM BOUNDED;

-- Filled while writable, then made read-only and re-attached: `startup` runs with `table_readonly = 1`,
-- and the bounded stream must read the initial snapshot and finish.
ALTER TABLE t_readonly_streaming MODIFY SETTING table_readonly = 0;
INSERT INTO t_readonly_streaming SELECT number, number * 10 FROM numbers(5);
INSERT INTO t_readonly_streaming SELECT number, number * 10 FROM numbers(5, 5);
ALTER TABLE t_readonly_streaming MODIFY SETTING table_readonly = 1;
DETACH TABLE t_readonly_streaming;
ATTACH TABLE t_readonly_streaming;

SELECT 'attached read-only', count(), sum(k), sum(v) FROM t_readonly_streaming STREAM BOUNDED;

-- Turning the setting back off must not disturb the streaming job that every table runs.
ALTER TABLE t_readonly_streaming MODIFY SETTING table_readonly = 0;
SELECT 'writable again', count(), sum(k), sum(v) FROM t_readonly_streaming STREAM BOUNDED;

DROP TABLE t_readonly_streaming;
