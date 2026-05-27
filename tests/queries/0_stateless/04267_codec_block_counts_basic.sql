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

-- Top-level codec_block_counts: codec name presence and that block counts are positive.
SELECT
    column,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_basic' AND active
ORDER BY column;

-- Subcolumns for the tuple: subcolumn names and per-subcolumn codec names.
SELECT
    `subcolumns.names`,
    arrayMap(m -> mapKeys(m), `subcolumns.codec_block_counts`) AS subcolumn_codecs
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_basic' AND active AND column = 't';

-- A non-projected query should still work and return correct row counts.
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_basic' AND active;

DROP TABLE t_basic;
