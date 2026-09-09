-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: this test checks codecs in Wide-part stream headers.

DROP TABLE IF EXISTS t_tuple_codec_transparent_wrappers;
DROP TABLE IF EXISTS t_tuple_codec_nullable_wrappers;
DROP TABLE IF EXISTS t_tuple_codec_null_modifier;
DROP TABLE IF EXISTS t_tuple_codec_default_nullable;

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

SET enable_tuple_element_codecs = 1;
SET enable_nullable_tuple_type = 1;

CREATE TABLE t_tuple_codec_nullable_wrappers
(
    key UInt64,
    top Nullable(Tuple(
        id UInt64 CODEC(Delta, LZ4),
        text String
    )) CODEC(ZSTD(1)),
    nested Tuple(
        record Nullable(Tuple(
            id UInt64 CODEC(T64, LZ4),
            text String
        )) CODEC(ZSTD(1))
    ),
    array_value Array(Nullable(Tuple(
        id UInt64 CODEC(Delta, LZ4),
        text String
    ))) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0;

INSERT INTO t_tuple_codec_nullable_wrappers VALUES
    (1, (10, 'top'), tuple((20, 'nested')), [(30, 'array'), NULL]),
    (2, NULL, tuple(NULL), []);

SELECT
    position(create_table_query, 'Nullable(Tuple(id UInt64 CODEC(Delta(8), LZ4)') > 0,
    position(create_table_query, 'record Nullable(Tuple(id UInt64 CODEC(T64, LZ4)') > 0,
    position(create_table_query, 'record Nullable(Tuple(id UInt64 CODEC(T64, LZ4), text String)) CODEC(ZSTD(1))') > 0,
    position(create_table_query, 'Array(Nullable(Tuple(id UInt64 CODEC(Delta(8), LZ4)') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_nullable_wrappers';

SELECT
    countIf(column = 'top' AND endsWith(substream, '.null') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0,
    countIf(column = 'top' AND endsWith(substream, '%2Eid') AND arrayExists(x -> startsWith(x, 'Delta('), mapKeys(codec_block_counts))) > 0,
    countIf(column = 'nested' AND endsWith(substream, '%2Erecord.null') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0,
    countIf(column = 'nested' AND endsWith(substream, '%2Erecord%2Eid') AND mapContains(codec_block_counts, 'T64, LZ4')) > 0,
    countIf(column = 'array_value' AND endsWith(substream, '.size0') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0,
    countIf(column = 'array_value' AND endsWith(substream, '.null') AND mapContains(codec_block_counts, 'ZSTD(1)')) > 0,
    countIf(column = 'array_value' AND endsWith(substream, '%2Eid') AND arrayExists(x -> startsWith(x, 'Delta('), mapKeys(codec_block_counts))) > 0
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_tuple_codec_nullable_wrappers);

SELECT
    count(),
    countIf(top IS NULL),
    sum(ifNull(top.id, 0)),
    countIf(nested.record IS NULL),
    sum(ifNull(nested.record.id, 0)),
    sum(length(array_value))
FROM t_tuple_codec_nullable_wrappers;

ALTER TABLE t_tuple_codec_nullable_wrappers
    MODIFY COLUMN top Nullable(Tuple(
        id UInt64 CODEC(ZSTD(2)),
        text String
    )) CODEC(ZSTD(1));

ALTER TABLE t_tuple_codec_nullable_wrappers
    MODIFY COLUMN nested Tuple(
        record Nullable(Tuple(
            id UInt64 REMOVE CODEC,
            text String
        ))
    );

SELECT
    position(create_table_query, 'Nullable(Tuple(id UInt64 CODEC(ZSTD(2))') > 0,
    position(create_table_query, 'record Nullable(Tuple(id UInt64 CODEC') = 0,
    position(create_table_query, 'record Nullable(Tuple(id UInt64, text String)) CODEC(ZSTD(1))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_nullable_wrappers';

CREATE TABLE t_tuple_codec_null_modifier
(
    value Tuple(id UInt64 CODEC(LZ4), text String) NULL
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_tuple_codec_null_modifier VALUES (NULL), ((1, 'one'));

SELECT
    type = 'Nullable(Tuple(id UInt64, text String))',
    position(
        (SELECT create_table_query FROM system.tables
         WHERE database = currentDatabase() AND name = 't_tuple_codec_null_modifier'),
        'id UInt64 CODEC(LZ4)') > 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_null_modifier' AND name = 'value';

ALTER TABLE t_tuple_codec_null_modifier
    MODIFY COLUMN value Tuple(id UInt64 CODEC(ZSTD(2)), text String) NULL;

SELECT position(create_table_query, 'id UInt64 CODEC(ZSTD(2))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_null_modifier';

ALTER TABLE t_tuple_codec_null_modifier
    MODIFY COLUMN value Tuple(id UInt64 REMOVE CODEC, text String) NULL;

SELECT position(create_table_query, 'id UInt64 CODEC') = 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_null_modifier';

ALTER TABLE t_tuple_codec_null_modifier
    ADD COLUMN added Tuple(id UInt64 CODEC(T64, LZ4), text String) NULL;

SELECT
    type = 'Nullable(Tuple(id UInt64, text String))',
    position(
        (SELECT create_table_query FROM system.tables
         WHERE database = currentDatabase() AND name = 't_tuple_codec_null_modifier'),
        'Nullable(Tuple(id UInt64 CODEC(T64, LZ4)') > 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_null_modifier' AND name = 'added';

SET data_type_default_nullable = 1;
CREATE TABLE t_tuple_codec_default_nullable
(
    value Tuple(id UInt64 CODEC(LZ4), text String)
)
ENGINE = MergeTree
ORDER BY tuple();
SET data_type_default_nullable = 0;

SELECT
    type = 'Nullable(Tuple(id UInt64, text String))',
    position(
        (SELECT create_table_query FROM system.tables
         WHERE database = currentDatabase() AND name = 't_tuple_codec_default_nullable'),
        'id UInt64 CODEC(LZ4)') > 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_default_nullable' AND name = 'value';

DETACH TABLE t_tuple_codec_nullable_wrappers;
SET enable_tuple_element_codecs = 0;
ATTACH TABLE t_tuple_codec_nullable_wrappers;

SELECT count() FROM t_tuple_codec_nullable_wrappers;

DROP TABLE t_tuple_codec_nullable_wrappers;
DROP TABLE t_tuple_codec_null_modifier;
DROP TABLE t_tuple_codec_default_nullable;
SET enable_nullable_tuple_type = 0;
