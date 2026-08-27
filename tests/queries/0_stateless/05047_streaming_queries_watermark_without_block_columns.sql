-- Tags: no-parallel-replicas
-- Materializing only the offset produces duplicate (_block_number, _block_offset)
-- pairs after a merge: the offsets keep the original per-insert values while the
-- block number falls back to a single value for the whole merged part. The duplicate
-- keys must not confuse the alignment of the data stream with the watermark markers.

SET enable_streaming_queries = 1;

CREATE TABLE t_streaming_duplicate_keys (value UInt64, event_time DateTime64(3))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1;

INSERT INTO t_streaming_duplicate_keys SELECT number, toDateTime64('2020-01-01 00:00:00', 3) + number FROM numbers(5);
INSERT INTO t_streaming_duplicate_keys SELECT number + 5, toDateTime64('2020-01-01 00:00:05', 3) + number FROM numbers(5);
INSERT INTO t_streaming_duplicate_keys SELECT number + 10, toDateTime64('2020-01-01 00:00:10', 3) + number FROM numbers(5);

OPTIMIZE TABLE t_streaming_duplicate_keys FINAL;

SELECT _block_number, _block_offset FROM t_streaming_duplicate_keys ORDER BY _block_number, _block_offset;

SELECT sum(value) FROM t_streaming_duplicate_keys STREAM BOUNDED WATERMARK FOR event_time AS event_time;

-- The filter keeps a single row of a duplicate-key run; the time attribute must be its own
-- event time, not the event time of a dropped twin with the same key.
SELECT value, _time_attribute FROM t_streaming_duplicate_keys STREAM BOUNDED WATERMARK FOR event_time AS event_time PREWHERE value = 10;

SELECT value, _time_attribute FROM (
    SELECT value, _time_attribute FROM t_streaming_duplicate_keys STREAM BOUNDED WATERMARK FOR event_time AS event_time WHERE value IN (0, 7, 14)
) ORDER BY value;
