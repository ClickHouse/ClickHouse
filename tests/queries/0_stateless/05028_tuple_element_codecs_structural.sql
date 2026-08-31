-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test associates codecs with Wide-part structural stream files.

DROP TABLE IF EXISTS t_tuple_codec_structural;

CREATE TABLE t_tuple_codec_structural
(
    key UInt64,
    payload Tuple(
        array_value Array(UInt64) CODEC(Delta, ZSTD(1)),
        nullable_value Nullable(UInt64) CODEC(Delta, LZ4),
        plain_array Array(String) CODEC(ZSTD(3))
    )
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_tuple_codec_structural
SELECT
    number,
    (
        range(number % 4 + 1),
        if(number % 2 = 0, NULL, number),
        arrayMap(x -> concat('value-', toString(x % 10)), range(number % 3 + 1))
    )
FROM numbers(100000);

SELECT
    substream,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_structural)
WHERE column = 'payload'
ORDER BY substream;

SELECT
    count() = 100000,
    min(length(payload.array_value)) = 1,
    max(length(payload.array_value)) = 4,
    countIf(isNull(payload.nullable_value)) = 50000,
    min(length(payload.plain_array)) = 1,
    max(length(payload.plain_array)) = 3
FROM t_tuple_codec_structural;

OPTIMIZE TABLE t_tuple_codec_structural FINAL;

SELECT
    substream,
    mapKeys(codec_block_counts) AS codecs,
    arrayMin(mapValues(codec_block_counts)) > 0 AS all_positive
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_structural)
WHERE column = 'payload'
ORDER BY substream;

SELECT count() = 100000, countIf(isNull(payload.nullable_value)) = 50000
FROM t_tuple_codec_structural;

DROP TABLE t_tuple_codec_structural;
