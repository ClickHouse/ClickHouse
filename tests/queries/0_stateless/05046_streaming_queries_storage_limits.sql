-- Tags: no-parallel-replicas
-- Read limits must not truncate a read round mid-way (a partially sorted metadata
-- stream would have holes breaking stream alignment). The limits are checked by
-- the commit order source against the delivered rows instead: break mode
-- finishes the stream cleanly, throw mode fails the query.
-- sum() is used instead of count() because the trivial count optimization
-- answers count() from metadata without executing the streaming read.

SET enable_streaming_queries = 1;

CREATE TABLE t_streaming_storage_limits (value UInt64, event_time DateTime64(3))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_storage_limits SELECT number, toDateTime64('2020-01-01 00:00:00', 3) + number FROM numbers(1000);

-- Break mode: the stream ends cleanly, no exception and no alignment error.
SELECT sum(value) <= 499500 FROM t_streaming_storage_limits STREAM BOUNDED
SETTINGS max_rows_to_read = 10, read_overflow_mode = 'break';

SELECT sum(value) <= 499500 FROM t_streaming_storage_limits STREAM BOUNDED
SETTINGS max_rows_to_read_leaf = 10, read_overflow_mode_leaf = 'break';

SELECT sum(value) <= 499500 FROM t_streaming_storage_limits STREAM BOUNDED WATERMARK FOR event_time AS event_time
SETTINGS max_rows_to_read = 10, read_overflow_mode = 'break';

-- Throw mode: the limit fails the query.
SELECT sum(value) FROM t_streaming_storage_limits STREAM BOUNDED
SETTINGS max_rows_to_read = 10, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

SELECT sum(value) FROM t_streaming_storage_limits STREAM BOUNDED
SETTINGS max_rows_to_read_leaf = 10, read_overflow_mode_leaf = 'throw'; -- { serverError TOO_MANY_ROWS }

SELECT sum(value) FROM t_streaming_storage_limits STREAM BOUNDED WATERMARK FOR event_time AS event_time
SETTINGS max_rows_to_read = 10, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
