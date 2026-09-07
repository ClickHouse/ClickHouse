-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test checks codecs in Wide-part stream headers.

DROP TABLE IF EXISTS t_tuple_codec_transparent_wrappers;

SET enable_tuple_element_codecs = 1;

CREATE TABLE t_tuple_codec_transparent_wrappers
(
    key UInt64,
    array_value Array(Tuple(
        a UInt64 CODEC(Delta, ZSTD(1)),
        b UInt64 CODEC(LZ4HC(4)),
        c UInt64
    )) CODEC(ZSTD(3)),
    aggregate_value SimpleAggregateFunction(any, Array(Tuple(
        a UInt64 CODEC(T64, LZ4),
        b UInt64 CODEC(ZSTD(2)),
        c UInt64
    ))) CODEC(LZ4HC(2))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0;

INSERT INTO t_tuple_codec_transparent_wrappers
SELECT
    number,
    [(number, number + 1, number + 2), (number + 3, number + 4, number + 5)],
    [(number + 6, number + 7, number + 8)]
FROM numbers(100000);

SELECT
    position(create_table_query, 'Array(Tuple(a UInt64 CODEC(Delta(8), ZSTD(1))') > 0,
    position(create_table_query, 'b UInt64 CODEC(LZ4HC(4))') > 0,
    position(create_table_query, 'CODEC(ZSTD(3))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_transparent_wrappers';

SELECT
    position(create_table_query, 'SimpleAggregateFunction(any, Array(Tuple(a UInt64 CODEC(T64, LZ4)') > 0,
    position(create_table_query, 'b UInt64 CODEC(ZSTD(2))') > 0,
    position(create_table_query, 'CODEC(LZ4HC(2))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_transparent_wrappers';

-- Block headers identify decoders, not encoder parameters. ZSTD levels are read as the
-- default ZSTD(1), and LZ4HC uses the same on-disk method byte as LZ4.
SELECT
    countIf(column = 'array_value' AND endsWith(substream, '%2Ea') AND arrayExists(x -> startsWith(x, 'Delta('), mapKeys(codec_block_counts))) > 0,
    countIf(column = 'array_value' AND endsWith(substream, '%2Eb') AND mapContains(codec_block_counts, 'LZ4')) > 0,
    countIf(column = 'array_value' AND endsWith(substream, '.size0') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0,
    countIf(column = 'aggregate_value' AND endsWith(substream, '%2Ea') AND mapContains(codec_block_counts, 'T64, LZ4')) > 0,
    countIf(column = 'aggregate_value' AND endsWith(substream, '%2Eb') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0,
    countIf(column = 'aggregate_value' AND endsWith(substream, '.size0') AND mapContains(codec_block_counts, 'LZ4')) > 0
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_transparent_wrappers);

SELECT
    count() = 100000,
    sum(arraySum(arrayMap(x -> x.a, array_value))) = 10000200000,
    sum(arraySum(arrayMap(x -> x.c, aggregate_value))) = 5000750000
FROM t_tuple_codec_transparent_wrappers;

ALTER TABLE t_tuple_codec_transparent_wrappers
    MODIFY COLUMN array_value Array(Tuple(
        a UInt64,
        b UInt64 REMOVE CODEC,
        c UInt64 CODEC(T64, LZ4)
    ));

ALTER TABLE t_tuple_codec_transparent_wrappers
    MODIFY COLUMN aggregate_value SimpleAggregateFunction(any, Array(Tuple(
        a UInt64,
        b UInt64 REMOVE CODEC,
        c UInt64 CODEC(ZSTD(4))
    )));

SELECT
    position(create_table_query, 'a UInt64 CODEC(Delta(8), ZSTD(1))') > 0,
    position(create_table_query, 'b UInt64 CODEC') = 0,
    position(create_table_query, 'c UInt64 CODEC(T64, LZ4)') > 0,
    position(create_table_query, 'CODEC(ZSTD(3))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_transparent_wrappers';

SELECT
    position(create_table_query, 'a UInt64 CODEC(T64, LZ4)') > 0,
    position(create_table_query, 'b UInt64 CODEC') = 0,
    position(create_table_query, 'c UInt64 CODEC(ZSTD(4))') > 0,
    position(create_table_query, 'CODEC(LZ4HC(2))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_transparent_wrappers';

INSERT INTO t_tuple_codec_transparent_wrappers
SELECT
    number + 100000,
    [(number, number + 1, number + 2)],
    [(number + 3, number + 4, number + 5)]
FROM numbers(1000);

SELECT
    countIf(column = 'array_value' AND endsWith(substream, '%2Ec') AND mapContains(codec_block_counts, 'T64, LZ4')) > 0,
    countIf(column = 'aggregate_value' AND endsWith(substream, '%2Ec') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_transparent_wrappers);

DETACH TABLE t_tuple_codec_transparent_wrappers;
SET enable_tuple_element_codecs = 0;
ATTACH TABLE t_tuple_codec_transparent_wrappers;

SELECT count(), sum(length(array_value)), sum(length(aggregate_value))
FROM t_tuple_codec_transparent_wrappers;

DROP TABLE t_tuple_codec_transparent_wrappers;
