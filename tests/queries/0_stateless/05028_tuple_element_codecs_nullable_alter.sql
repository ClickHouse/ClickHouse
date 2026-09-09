DROP TABLE IF EXISTS t_tuple_codec_nullable_alter;

SET enable_nullable_tuple_type = 1;
SET enable_tuple_element_codecs = 1;
SET mutations_sync = 2;

CREATE TABLE t_tuple_codec_nullable_alter
(
    value Tuple(
        id UInt64 CODEC(ZSTD(2)),
        text String
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_tuple_codec_nullable_alter VALUES ((1, 'one'));

-- Preserving an existing declaration does not require enabling creation of new declarations.
SET enable_tuple_element_codecs = 0;

-- Adding a transparent Nullable wrapper preserves an omitted Tuple-element declaration.
ALTER TABLE t_tuple_codec_nullable_alter
    MODIFY COLUMN value Nullable(Tuple(
        id UInt64,
        text String
    ));

SELECT
    type = 'Nullable(Tuple(id UInt64, text String))',
    position(
        (SELECT create_table_query FROM system.tables
         WHERE database = currentDatabase() AND name = 't_tuple_codec_nullable_alter'),
        'id UInt64 CODEC(ZSTD(2))') > 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_nullable_alter' AND name = 'value';

SELECT count() = 1, countIf(value = (1, 'one')) = 1
FROM t_tuple_codec_nullable_alter;

-- Removing the wrapper has the same preservation rule. DEFAULT is unrelated column metadata
-- and must not make the omitted codec declaration look like a removal.
ALTER TABLE t_tuple_codec_nullable_alter
    MODIFY COLUMN value Tuple(
        id UInt64,
        text String
    ) DEFAULT (0, '');

SELECT
    type = 'Tuple(id UInt64, text String)',
    default_kind = 'DEFAULT',
    position(
        (SELECT create_table_query FROM system.tables
         WHERE database = currentDatabase() AND name = 't_tuple_codec_nullable_alter'),
        'id UInt64 CODEC(ZSTD(2))') > 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_nullable_alter' AND name = 'value';

SELECT count() = 1, countIf(value = (1, 'one')) = 1
FROM t_tuple_codec_nullable_alter;

DROP TABLE t_tuple_codec_nullable_alter;

SET enable_tuple_element_codecs = 0;
SET enable_nullable_tuple_type = 0;
