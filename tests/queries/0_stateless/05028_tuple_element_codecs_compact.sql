-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test forces Compact parts and asserts empty per-stream codec maps.

DROP TABLE IF EXISTS t_tuple_codec_compact;

CREATE TABLE t_tuple_codec_compact
(
    key UInt64,
    payload Tuple(
        number UInt64 CODEC(Delta, LZ4),
        text String CODEC(ZSTD(1)),
        inherited UInt64
    ) CODEC(LZ4HC(4))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000;

SYSTEM STOP MERGES t_tuple_codec_compact;

INSERT INTO t_tuple_codec_compact SELECT number, (number, toString(number % 100), number * 2) FROM numbers(1000);
INSERT INTO t_tuple_codec_compact SELECT number + 1000, (number + 1000, toString(number % 100), (number + 1000) * 2) FROM numbers(1000);
INSERT INTO t_tuple_codec_compact SELECT number + 2000, (number + 2000, toString(number % 100), (number + 2000) * 2) FROM numbers(1000);

SELECT count(), groupUniqArray(part_type)
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_compact' AND active;

SELECT
    substream,
    count() AS parts,
    countIf(empty(codec_block_counts)) AS empty_maps,
    countIf(isNull(data_compressed_bytes) AND isNull(data_uncompressed_bytes)) AS null_sizes
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_compact)
WHERE column = 'payload'
GROUP BY substream
ORDER BY substream;

SELECT
    count() = 3000,
    min(payload.number) = 0,
    max(payload.number) = 2999,
    uniqExact(payload.text) = 100,
    countIf(payload.inherited = key * 2) = 3000
FROM t_tuple_codec_compact;

SYSTEM START MERGES t_tuple_codec_compact;
OPTIMIZE TABLE t_tuple_codec_compact FINAL;

SELECT count(), groupUniqArray(part_type)
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_compact' AND active;

SELECT
    substream,
    empty(codec_block_counts),
    isNull(data_compressed_bytes) AND isNull(data_uncompressed_bytes)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_compact)
WHERE column = 'payload'
ORDER BY substream;

ALTER TABLE t_tuple_codec_compact
    UPDATE payload = tuple(
        payload.number + if(key % 2 = 0, 1, 0),
        payload.text,
        payload.inherited + if(key % 2 = 0, 1, 0))
    WHERE 1
SETTINGS mutations_sync = 2;

SELECT count(), groupUniqArray(part_type)
FROM system.parts
WHERE database = currentDatabase() AND table = 't_tuple_codec_compact' AND active;

SELECT
    countIf(payload.number = key + if(key % 2 = 0, 1, 0)) = 3000,
    countIf(payload.inherited = key * 2 + if(key % 2 = 0, 1, 0)) = 3000
FROM t_tuple_codec_compact;

SELECT
    countSubstrings(create_table_query, 'CODEC(') = 3,
    position(create_table_query, 'number UInt64 CODEC(Delta(8), LZ4)') > 0,
    position(create_table_query, 'text String CODEC(ZSTD(1))') > 0,
    position(create_table_query, 'payload Tuple') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_compact';

DROP TABLE t_tuple_codec_compact;
