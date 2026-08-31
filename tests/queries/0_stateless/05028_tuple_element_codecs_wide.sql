-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test requires Wide parts and stable physical substream enumeration.

DROP TABLE IF EXISTS t_tuple_codec_wide;

CREATE TABLE t_tuple_codec_wide
(
    key UInt64,
    payload Tuple(
        transformed UInt64 CODEC(Delta, LZ4),
        inherited String,
        nested Tuple(
            transformed UInt32 CODEC(T64, LZ4),
            inherited String
        ),
        defaulted UInt64 CODEC(Default)
    ) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
    min_bytes_for_wide_part = 0,
    min_compress_block_size = 0,
    max_compress_block_size = 65536,
    default_compression_codec = 'LZ4';

SYSTEM STOP MERGES t_tuple_codec_wide;

INSERT INTO t_tuple_codec_wide
SELECT
    number,
    (
        number * 3,
        concat('inherited-', toString(number % 100)),
        (toUInt32(number % 10000), concat('nested-', toString(number % 50))),
        number + 7
    )
FROM numbers(100000);

SELECT
    substream,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_wide)
WHERE column = 'payload'
ORDER BY substream;

SELECT
    count() = 100000,
    min(payload.transformed) = 0,
    max(payload.transformed) = 299997,
    uniqExact(payload.inherited) = 100,
    groupBitXor(cityHash64(payload)) = groupBitXor(cityHash64(tuple(
        payload.transformed,
        payload.inherited,
        tuple(payload.nested.transformed, payload.nested.inherited),
        payload.defaulted)))
FROM t_tuple_codec_wide;

INSERT INTO t_tuple_codec_wide
SELECT
    number + 100000,
    (
        (number + 100000) * 3,
        concat('inherited-', toString(number % 100)),
        (toUInt32(number % 10000), concat('nested-', toString(number % 50))),
        number + 100007
    )
FROM numbers(100000);

SYSTEM START MERGES t_tuple_codec_wide;
OPTIMIZE TABLE t_tuple_codec_wide FINAL;

SELECT
    substream,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_wide)
WHERE column = 'payload'
ORDER BY substream;

SELECT count() = 200000, min(key) = 0, max(key) = 199999 FROM t_tuple_codec_wide;

DROP TABLE t_tuple_codec_wide;
