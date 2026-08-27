-- Tags: no-parallel-replicas
-- Materializing only the offset produces duplicate (_block_number, _block_offset)
-- pairs after a merge: the offsets keep the original per-insert values while the
-- block number falls back to a single value for the whole merged part. The
-- duplicates are identical in the metadata and the data stream, so alignment holds.

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
