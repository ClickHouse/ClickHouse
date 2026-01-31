-- Tags: no-random-mergetree-settings, no-random-settings, no-fasttest
-- ^^ Prevent the data sizes from varying with random parameters.
-- ^^ FastPFor codecs require the FastPFor library which is not built in fasttest.

-- This test validates the storage size of the text index with different posting list codecs.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET max_insert_threads = 1;
SET max_block_size = 65536;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

-- Create tables for all codecs

DROP TABLE IF EXISTS tab_uncompressed;
DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS tab_fastpfor;
DROP TABLE IF EXISTS tab_binarypacking;
DROP TABLE IF EXISTS tab_streamvbyte;
DROP TABLE IF EXISTS tab_optpfor;

CREATE TABLE tab_uncompressed
(
    ts DateTime CODEC(LZ4),
    str String CODEC(LZ4),
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'none'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part = 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   min_compress_block_size = 65536,
   max_compress_block_size = 1048576,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

CREATE TABLE tab_bitpacking
(
    ts DateTime CODEC(LZ4),
    str String CODEC(LZ4),
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part = 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   min_compress_block_size = 65536,
   max_compress_block_size = 1048576,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

CREATE TABLE tab_fastpfor
(
    ts DateTime CODEC(LZ4),
    str String CODEC(LZ4),
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'fastpfor'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part = 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   min_compress_block_size = 65536,
   max_compress_block_size = 1048576,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

CREATE TABLE tab_binarypacking
(
    ts DateTime CODEC(LZ4),
    str String CODEC(LZ4),
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'binarypacking'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part = 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   min_compress_block_size = 65536,
   max_compress_block_size = 1048576,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

CREATE TABLE tab_streamvbyte
(
    ts DateTime CODEC(LZ4),
    str String CODEC(LZ4),
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'streamvbyte'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part = 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   min_compress_block_size = 65536,
   max_compress_block_size = 1048576,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

CREATE TABLE tab_optpfor
(
    ts DateTime CODEC(LZ4),
    str String CODEC(LZ4),
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'optpfor'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part = 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   min_compress_block_size = 65536,
   max_compress_block_size = 1048576,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

-- Insert test data into all tables

-- Large posting lists (aa/bb/cc each ~341334 hits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_streamvbyte SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_optpfor SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);

-- Block boundary case (129 = 128 + 1 tail) and single hit
INSERT INTO tab_uncompressed SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_bitpacking SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_fastpfor SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_binarypacking SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_streamvbyte SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_optpfor SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);

-- Medium non-aligned (1003 hits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_bitpacking SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_fastpfor SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_binarypacking SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_streamvbyte SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_optpfor SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);

-- Very sparse lists (2 and 5 hits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_streamvbyte SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_optpfor SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);

-- Optimize all tables
OPTIMIZE TABLE tab_uncompressed FINAL;
OPTIMIZE TABLE tab_bitpacking FINAL;
OPTIMIZE TABLE tab_fastpfor FINAL;
OPTIMIZE TABLE tab_binarypacking FINAL;
OPTIMIZE TABLE tab_streamvbyte FINAL;
OPTIMIZE TABLE tab_optpfor FINAL;

-- Compare the size of the text index for the same dataset with different codecs
SELECT
    table,
    sum(rows),
    sum(secondary_indices_compressed_bytes)
FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('tab_uncompressed', 'tab_bitpacking', 'tab_fastpfor', 'tab_binarypacking', 'tab_streamvbyte', 'tab_optpfor')
GROUP BY table
ORDER BY table;

-- Cleanup
DROP TABLE tab_uncompressed;
DROP TABLE tab_bitpacking;
DROP TABLE tab_fastpfor;
DROP TABLE tab_binarypacking;
DROP TABLE tab_streamvbyte;
DROP TABLE tab_optpfor;
