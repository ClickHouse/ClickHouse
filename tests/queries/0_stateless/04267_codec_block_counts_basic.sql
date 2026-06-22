-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Compact, where codec_block_counts is empty.

DROP TABLE IF EXISTS t_basic;

CREATE TABLE t_basic
(
    a UInt64 CODEC(LZ4),
    b UInt64 CODEC(Delta, LZ4),
    t Tuple(x UInt32, y UInt32) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_basic
SELECT number, number, (toUInt32(number), toUInt32(number * 2))
FROM numbers(100000);

-- One row per (column, substream): the tuple `t` is two streams `t.x`, `t.y`.
SELECT
    column,
    substream,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_basic)
ORDER BY column, substream;

-- The tuple's streams listed on their own.
SELECT
    substream,
    mapKeys(codec_block_counts) AS codecs
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_basic)
WHERE column = 't'
ORDER BY substream;

-- A query that selects no codec column stays metadata-only and still returns one row per (part, column, substream).
SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_basic);

SELECT substream, data_uncompressed_bytes, data_compressed_bytes > 0 AS compressed_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_basic)
ORDER BY substream;

DROP TABLE t_basic;
