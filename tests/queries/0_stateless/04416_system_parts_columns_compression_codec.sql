-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS test_system_parts_columns_compression_codec_wide;
DROP TABLE IF EXISTS test_system_parts_columns_compression_codec_compact;

CREATE TABLE test_system_parts_columns_compression_codec_wide
(
    p UInt8,
    s String,
    t String CODEC(ZSTD(5)),
    d Date CODEC(Delta, LZ4HC(3))
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4';

CREATE TABLE test_system_parts_columns_compression_codec_compact
(
    p UInt8,
    s String,
    t String CODEC(ZSTD(5)),
    d Date CODEC(Delta, LZ4HC(3))
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    default_compression_codec = 'ZSTD(1)';

INSERT INTO test_system_parts_columns_compression_codec_wide VALUES (1, 'a', 'b', '2020-01-01');
INSERT INTO test_system_parts_columns_compression_codec_compact VALUES (1, 'a', 'b', '2020-01-01');

-- A column without its own CODEC reports the default compression codec of the part, which follows
-- the `default_compression_codec` setting and so differs between the two tables.
SELECT 'wide', column, compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_system_parts_columns_compression_codec_wide'
    AND active
ORDER BY column;

SELECT 'compact', column, compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_system_parts_columns_compression_codec_compact'
    AND active
ORDER BY column;

-- The value follows the current column definition. The already written part keeps the old codec on
-- disk until a merge or mutation rewrites it, which this column does not claim to report.
ALTER TABLE test_system_parts_columns_compression_codec_wide MODIFY COLUMN s String CODEC(ZSTD(3));

SELECT 'after_alter', column, compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_system_parts_columns_compression_codec_wide'
    AND active
    AND column = 's';

DROP TABLE test_system_parts_columns_compression_codec_wide;
DROP TABLE test_system_parts_columns_compression_codec_compact;
