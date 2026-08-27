-- Tags: no-parallel-replicas, no-old-analyzer

SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_stream_banned;

CREATE TABLE t_stream_banned (id UInt64, ts DateTime64(3), s String)
ENGINE = ReplacingMergeTree
ORDER BY (id, intHash32(id))
SAMPLE BY intHash32(id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- STREAM is not compatible with other table expression modifiers.
SELECT * FROM t_stream_banned FINAL STREAM; -- { serverError SYNTAX_ERROR }
SELECT * FROM t_stream_banned SAMPLE 1/2 STREAM; -- { serverError SYNTAX_ERROR }

-- WATERMARK requires commit-order sorted streams.
SELECT * FROM t_stream_banned STREAM UNORDERED WATERMARK FOR ts AS ts; -- { serverError ILLEGAL_STREAM }

-- WATERMARK column must exist and be a date or time type.
SELECT * FROM t_stream_banned STREAM WATERMARK FOR unknown AS unknown; -- { serverError ILLEGAL_STREAM }
SELECT * FROM t_stream_banned STREAM WATERMARK FOR s AS s; -- { serverError ILLEGAL_STREAM }

-- WATERMARK expression result type must match the column type.
SELECT * FROM t_stream_banned STREAM WATERMARK FOR ts AS toDateTime(ts); -- { serverError ILLEGAL_STREAM }
