-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the part to Compact, where codec_block_counts is empty.

DROP TABLE IF EXISTS t_basic;
DROP TABLE IF EXISTS t_not_mergetree;

-- Wrong number of arguments.
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase()); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Not a MergeTree table.
CREATE TABLE t_not_mergetree (a UInt64) ENGINE = Memory;
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_not_mergetree); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_not_mergetree;

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

-- A query that selects no codec column stays metadata-only and still returns one row per (part, column, substream).
SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_basic);

SELECT substream, data_uncompressed_bytes, round(data_compressed_bytes / data_uncompressed_bytes, 2) AS compression_ratio
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_basic)
ORDER BY substream;

DROP TABLE t_basic;
