-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Wide and break the empty-maps assertion.

-- Compact parts interleave all columns into a single data.bin, so per-column codec attribution from block headers is not possible.

DROP TABLE IF EXISTS t_compact;

CREATE TABLE t_compact (a UInt64 CODEC(LZ4), b UInt64 CODEC(ZSTD))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_compact SELECT number, number FROM numbers(1000);

-- Confirm the part is Compact (the function has no part_type column).
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact' AND active;

-- Streams are still listed (one row per substream), but Compact has no per-stream `.bin`: sizes are NULL and the codec map is empty.
SELECT column, substream, data_compressed_bytes, data_uncompressed_bytes, codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_compact)
ORDER BY column, substream;

DROP TABLE t_compact;
