-- Random settings limits: use_uncompressed_cache=(0, 0)

-- Compact part: the substreams of `a` are in separate compressed blocks (sizes: ZSTD, elements: Delta + ZSTD),
-- and the subcolumn `c.y` starts at a non-zero offset of the block shared with `c.x`.
DROP TABLE IF EXISTS t_stale_block;
CREATE TABLE t_stale_block (a Array(UInt32) CODEC(Delta, ZSTD), c Tuple(x UInt64, y UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    write_marks_for_substreams_in_compact_parts = 1, compress_per_column_in_compact_parts = 1,
    enable_adaptive_codec_selection = 0, ratio_of_defaults_for_sparse_serialization = 1,
    default_compression_codec = 'LZ4', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_stale_block VALUES ([1], (10, 20));
SELECT a, c.y FROM t_stale_block;
DROP TABLE t_stale_block;

-- #118560: with `compress_per_column_in_compact_parts = 0`, `agg_simple` and `agg` share one block.
DROP TABLE IF EXISTS t_stale_block_mutation;
CREATE TABLE t_stale_block_mutation
(
    time DateTime,
    xxx String,
    two_values Tuple(Array(UInt16), UInt32) CODEC(Delta, ZSTD),
    agg_simple SimpleAggregateFunction(sum, UInt64),
    agg SimpleAggregateFunction(sumMap, Tuple(Array(Int16), Array(UInt64)))
)
ENGINE = MergeTree ORDER BY (xxx, time)
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    write_marks_for_substreams_in_compact_parts = 1, compress_per_column_in_compact_parts = 0,
    enable_adaptive_codec_selection = 0, ratio_of_defaults_for_sparse_serialization = 1,
    default_compression_codec = 'LZ4', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_stale_block_mutation SELECT toDateTime(0), 'hello', ([min(number)], sum(number)), sum(number),
    sumMap((arrayMap(i -> toInt16(i), range(2)), arrayMap(i -> (number + i), range(2)))) FROM numbers(10);
ALTER TABLE t_stale_block_mutation UPDATE two_values = (two_values.1, two_values.2 + 1) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_stale_block_mutation UPDATE agg = (agg.1, arrayMap(v -> v + 1, agg.2)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT two_values, agg_simple, agg FROM t_stale_block_mutation;
DROP TABLE t_stale_block_mutation;
