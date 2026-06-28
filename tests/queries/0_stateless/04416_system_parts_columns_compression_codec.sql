DROP TABLE IF EXISTS test_system_parts_columns_compression_codec_wide;
DROP TABLE IF EXISTS test_system_parts_columns_compression_codec_compact;

CREATE TABLE test_system_parts_columns_compression_codec_wide
(
    p UInt8,
    s String
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4';

CREATE TABLE test_system_parts_columns_compression_codec_compact
(
    p UInt8,
    s String
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    compress_per_column_in_compact_parts = 1,
    default_compression_codec = 'LZ4';

INSERT INTO test_system_parts_columns_compression_codec_wide VALUES (1, 'old');
ALTER TABLE test_system_parts_columns_compression_codec_wide MODIFY COLUMN s String CODEC(ZSTD(3));
INSERT INTO test_system_parts_columns_compression_codec_wide VALUES (2, 'new');

INSERT INTO test_system_parts_columns_compression_codec_compact VALUES (1, 'old');
ALTER TABLE test_system_parts_columns_compression_codec_compact MODIFY COLUMN s String CODEC(ZSTD(3));
INSERT INTO test_system_parts_columns_compression_codec_compact VALUES (2, 'new');

SELECT
    'wide',
    count(),
    countIf(position(compression_codec, 'LZ4') > 0),
    countIf(position(compression_codec, 'ZSTD') > 0),
    uniqExact(compression_codec)
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_system_parts_columns_compression_codec_wide'
    AND active
    AND column = 's';

SELECT
    'compact',
    count(),
    countIf(position(compression_codec, 'LZ4') > 0),
    countIf(position(compression_codec, 'ZSTD') > 0),
    uniqExact(compression_codec)
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_system_parts_columns_compression_codec_compact'
    AND active
    AND column = 's';

DROP TABLE test_system_parts_columns_compression_codec_wide;
DROP TABLE test_system_parts_columns_compression_codec_compact;
