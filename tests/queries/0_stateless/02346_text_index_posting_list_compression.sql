-- This test compares two identical tables that differ only in the text index posting-list compression setting (posting_list_codec).
-- The inserted data covers a range of posting-list shapes:
-- - very large lists (aa/bb/cc),
-- - a block-boundary case (129 hits = 128 + tail),
-- - a medium non-aligned size (1003 hits),
-- - a single-hit list, and
-- - very sparse lists (2 and 5 hits).
-- After OPTIMIZE FINAL to stabilize on-disk parts, each hasToken() query validates that the compressed
-- and uncompressed tables return identical counts (and matches expected exact counts for key tokens),
-- ensuring correctness across full blocks, tail blocks, and small-N cases.

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET min_compress_block_size = 65536;
SET max_compress_block_size = 1048576;

DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS table_uncompressed;

-- The following MergeTree settings do NOT directly affect postings list compression.
-- They are pinned here because this test compares the on-disk data size for identical input
-- across two scenarios. Some of these settings can change the part layout/serialization,
-- compression granularity, or statistics, which in turn may alter the resulting data size.
-- By fixing them, we make the test deterministic and ensure predictable, stable output.
CREATE TABLE tab_bitpacking
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part= 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

CREATE TABLE table_uncompressed
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha'
    )
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
   min_rows_for_wide_part = 0,
   min_bytes_for_wide_part= 0,
   index_granularity = 8192,
   index_granularity_bytes = 0,
   enable_block_offset_column = 0,
   enable_block_number_column = 0,
   string_serialization_version = 'with_size_stream',
   primary_key_compress_block_size = 65536,
   marks_compress_block_size = 65536,
   ratio_of_defaults_for_sparse_serialization = 0.95,
   serialization_info_version = 'basic',
   auto_statistics_types = 'minmax';

INSERT INTO tab_bitpacking
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO table_uncompressed
SELECT
    '2026-01-09 11:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_bitpacking
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO table_uncompressed
SELECT
    '2026-01-09 13:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_bitpacking
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

INSERT INTO table_uncompressed
SELECT
    '2026-01-09 15:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

INSERT INTO tab_bitpacking
SELECT
    '2026-01-09 16:00:00',
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
FROM numbers(2000);

INSERT INTO table_uncompressed
SELECT
    '2026-01-09 17:00:00',
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
FROM numbers(2000);

OPTIMIZE TABLE tab_bitpacking FINAL;
OPTIMIZE TABLE table_uncompressed FINAL;

-- Compare the size of the inverted index for the same dataset with compression enabled versus disabled.

SELECT
    `table`,
    sum(rows) AS `rows-count`,
    sum(bytes_on_disk) AS `total-bytes`,
    sum(secondary_indices_compressed_bytes) AS `text-index-bytes`
FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('tab_bitpacking','table_uncompressed')
GROUP BY `table`;

-- Validates that a very large/high-frequency posting list is decoded correctly by checking the count in the compressed table matches the uncompressed baseline.

SELECT
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'aa')) AS count_bitpacking,
    (SELECT count() FROM table_uncompressed WHERE hasToken(str, 'aa')) AS count_uncompressed,
    (count_bitpacking = count_uncompressed) AS ok_aa;

-- Tests the block-boundary case (expected 129 hits, i.e. one full 128-value block plus a 1-value tail) and verifies compressed vs uncompressed counts are identical.

SELECT
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'tail129')) AS count_bitpacking,
    (SELECT count() FROM table_uncompressed WHERE hasToken(str, 'tail129')) AS count_uncompressed,
    count_bitpacking, count_uncompressed,
    (count_bitpacking = 129) AS ok_tail129,
    (count_bitpacking = count_uncompressed) AS ok_tail129_eq;

-- Tests a medium-length, non-aligned posting list (expected 1003 hits) to cover multi-block + non-trivial tail handling; also checks equality with the uncompressed table.

SELECT
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'mid1003')) AS count_bitpacking,
    (SELECT count() FROM table_uncompressed WHERE hasToken(str, 'mid1003')) AS count_uncompressed,
    (count_bitpacking = 1003) AS ok_mid1003,
    (count_bitpacking = count_uncompressed) AS ok_mid1003_eq;

-- Tests the single-element posting list case (expected 1 hit) and ensures compressed and uncompressed results match.

SELECT
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'single')) AS count_bitpacking,
    (SELECT count() FROM table_uncompressed WHERE hasToken(str, 'single')) AS count_uncompressed,
    (count_bitpacking = 1) AS ok_single,
    (count_bitpacking = count_uncompressed) AS ok_single_eq;

-- Tests a very small/sparse posting list (expected 2 hits) and checks compressed equals uncompressed.

SELECT
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'rare2')) AS count_bitpacking,
    (SELECT count() FROM table_uncompressed WHERE hasToken(str, 'rare2')) AS count_uncompressed,
    (count_bitpacking = 2) AS ok_rare2,
    (count_bitpacking = count_uncompressed) AS ok_rare2_eq;

-- Tests another small-N posting list (expected 5 hits) to cover additional short-list behavior; also checks compressed equals uncompressed.

SELECT
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'rare5')) AS count_bitpacking,
    (SELECT count() FROM table_uncompressed WHERE hasToken(str, 'rare5')) AS count_uncompressec,
    (count_bitpacking = 5) AS ok_rare5,
    (count_bitpacking = count_uncompressec) AS ok_rare5_eq;

DROP TABLE tab_bitpacking;
DROP TABLE table_uncompressed;
