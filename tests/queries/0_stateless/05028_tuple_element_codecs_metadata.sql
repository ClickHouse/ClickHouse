DROP TABLE IF EXISTS t_tuple_codec_metadata;
DROP TABLE IF EXISTS t_tuple_codec_metadata_renamed;

CREATE TABLE t_tuple_codec_metadata
(
    key UInt64,
    payload Tuple(
        id UInt64 CODEC(Delta, ZSTD(1)),
        text String CODEC(ZSTD(3)),
        nested Tuple(
            score Float64 CODEC(Gorilla, ZSTD(1)),
            label String
        ),
        defaulted UInt32 CODEC(Default),
        `literal.dot` String CODEC(LZ4HC(4))
    ) CODEC(LZ4),
    pair Tuple(
        UInt64 CODEC(T64, LZ4),
        String CODEC(ZSTD(2))
    )
)
ENGINE = MergeTree
ORDER BY key;

-- `SHOW CREATE` stores annotations locally in the tuple declaration, including literal-dot and positional elements.
SELECT
    countSubstrings(create_table_query, 'CODEC(') = 8,
    position(create_table_query, '`literal.dot` String CODEC(LZ4HC(4))') > 0,
    position(create_table_query, 'UInt64 CODEC(T64, LZ4)') > 0,
    position(create_table_query, 'payload.id') = 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_metadata';

-- Logical types remain undecorated while `compression_codec` contains the complete policy.
SELECT
    name,
    position(type, 'CODEC') = 0 AS undecorated_type,
    countSubstrings(compression_codec, 'CODEC(') AS declarations,
    position(compression_codec, '`literal.dot` String CODEC(LZ4HC(4))') > 0 AS has_literal_dot,
    position(compression_codec, 'score Float64 CODEC(Gorilla(8), ZSTD(1))') > 0 AS has_nested
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_metadata'
ORDER BY position;

DESCRIBE TABLE t_tuple_codec_metadata FORMAT JSONEachRow;
DESCRIBE TABLE t_tuple_codec_metadata FORMAT JSONEachRow
SETTINGS describe_include_subcolumns = 1;

INSERT INTO t_tuple_codec_metadata VALUES
    (1, (11, 'text', (1.5, 'label'), 7, 'dot'), (21, 'pair'));

SELECT toTypeName(payload), toTypeName(pair) FROM t_tuple_codec_metadata;

DETACH TABLE t_tuple_codec_metadata;
ATTACH TABLE t_tuple_codec_metadata;

SELECT
    countSubstrings(create_table_query, 'CODEC(') = 8,
    position(create_table_query, '`literal.dot` String CODEC(LZ4HC(4))') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_metadata';

RENAME TABLE t_tuple_codec_metadata TO t_tuple_codec_metadata_renamed;
RENAME TABLE t_tuple_codec_metadata_renamed TO t_tuple_codec_metadata;

SELECT
    name,
    countSubstrings(compression_codec, 'CODEC(') AS declarations
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_metadata'
ORDER BY position;

DROP TABLE t_tuple_codec_metadata;
