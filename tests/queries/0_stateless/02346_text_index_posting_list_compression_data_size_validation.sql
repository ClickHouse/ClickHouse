-- Tags: no-random-mergetree-settings, no-random-settings, no-fasttest
-- ^^ Prevent the data sizes from varying with random parameters.
-- ^^ FastPFor codecs require the FastPFor library which is not built in fasttest.

-- This test validates the storage size of the text index with different posting list codecs.
--
-- Key insight: The 'none' codec uses Roaring Bitmap which has excellent run-length encoding
-- for consecutive values. To demonstrate bitpacking compression benefits, we need:
-- 1. Large gaps between postings (to defeat Roaring's RLE)
-- 2. Uniform gap sizes (to minimize bits needed per delta)
-- 3. Large posting lists (to amortize fixed overhead)
--
-- Scenario 1: Large uniform gaps (delta = 100)
--   - 10 tokens, each with 100K postings, gap = 100
--   - Roaring cannot use RLE, must store individual values
--   - Bitpacking: delta=100 needs 7 bits -> ~4.5x compression
--
-- Scenario 2: Very large uniform gaps (delta = 1000)
--   - 10 tokens, each with 100K postings, gap = 1000
--   - Bitpacking: delta=1000 needs 10 bits -> ~3.2x compression
--
-- Scenario 3: Huge uniform gaps (delta = 10000)
--   - 10 tokens, each with 100K postings, gap = 10000
--   - Bitpacking: delta=10000 needs 14 bits -> ~2.3x compression

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET max_insert_threads = 1;
SET max_block_size = 65536;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

-- ============================================================================
-- Scenario 1: Large uniform gaps (delta = 100, needs 7 bits)
-- ============================================================================

DROP TABLE IF EXISTS gap100_none;
DROP TABLE IF EXISTS gap100_bitpacking;
DROP TABLE IF EXISTS gap100_fastpfor;
DROP TABLE IF EXISTS gap100_binarypacking;
DROP TABLE IF EXISTS gap100_optpfor;

CREATE TABLE gap100_none
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap100_bitpacking
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap100_fastpfor
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'fastpfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap100_binarypacking
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'binarypacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap100_optpfor
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'optpfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

-- 10 tokens (word_0..word_9), each appears every 100th row -> delta = 100
INSERT INTO gap100_none SELECT number, concat('word_', toString(number % 10)) FROM numbers(100000);
INSERT INTO gap100_bitpacking SELECT number, concat('word_', toString(number % 10)) FROM numbers(100000);
INSERT INTO gap100_fastpfor SELECT number, concat('word_', toString(number % 10)) FROM numbers(100000);
INSERT INTO gap100_binarypacking SELECT number, concat('word_', toString(number % 10)) FROM numbers(100000);
INSERT INTO gap100_optpfor SELECT number, concat('word_', toString(number % 10)) FROM numbers(100000);

OPTIMIZE TABLE gap100_none FINAL;
OPTIMIZE TABLE gap100_bitpacking FINAL;
OPTIMIZE TABLE gap100_fastpfor FINAL;
OPTIMIZE TABLE gap100_binarypacking FINAL;
OPTIMIZE TABLE gap100_optpfor FINAL;

SELECT 'Scenario 1: Gap=100 (10 tokens, 100K postings each, 7 bits/delta)';

SELECT
    replaceOne(table, 'gap100_', '') AS codec,
    sum(rows) AS rows,
    sum(secondary_indices_compressed_bytes) AS index_size
FROM system.parts
WHERE database = currentDatabase() AND active AND table LIKE 'gap100_%'
GROUP BY table
ORDER BY table;

DROP TABLE gap100_none;
DROP TABLE gap100_bitpacking;
DROP TABLE gap100_fastpfor;
DROP TABLE gap100_binarypacking;
DROP TABLE gap100_optpfor;

-- ============================================================================
-- Scenario 2: Very large uniform gaps (delta = 1000, needs 10 bits)
-- ============================================================================

DROP TABLE IF EXISTS gap1k_none;
DROP TABLE IF EXISTS gap1k_bitpacking;
DROP TABLE IF EXISTS gap1k_fastpfor;
DROP TABLE IF EXISTS gap1k_binarypacking;
DROP TABLE IF EXISTS gap1k_optpfor;

CREATE TABLE gap1k_none
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap1k_bitpacking
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap1k_fastpfor
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'fastpfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap1k_binarypacking
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'binarypacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap1k_optpfor
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'optpfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

-- 100 tokens (word_0..word_99), each appears every 1000th row -> delta = 1000
INSERT INTO gap1k_none SELECT number, concat('word_', toString(number % 100)) FROM numbers(100000);
INSERT INTO gap1k_bitpacking SELECT number, concat('word_', toString(number % 100)) FROM numbers(100000);
INSERT INTO gap1k_fastpfor SELECT number, concat('word_', toString(number % 100)) FROM numbers(100000);
INSERT INTO gap1k_binarypacking SELECT number, concat('word_', toString(number % 100)) FROM numbers(100000);
INSERT INTO gap1k_optpfor SELECT number, concat('word_', toString(number % 100)) FROM numbers(100000);

OPTIMIZE TABLE gap1k_none FINAL;
OPTIMIZE TABLE gap1k_bitpacking FINAL;
OPTIMIZE TABLE gap1k_fastpfor FINAL;
OPTIMIZE TABLE gap1k_binarypacking FINAL;
OPTIMIZE TABLE gap1k_optpfor FINAL;

SELECT 'Scenario 2: Gap=1000 (100 tokens, 100K postings each, 10 bits/delta)';

SELECT
    replaceOne(table, 'gap1k_', '') AS codec,
    sum(rows) AS rows,
    sum(secondary_indices_compressed_bytes) AS index_size
FROM system.parts
WHERE database = currentDatabase() AND active AND table LIKE 'gap1k_%'
GROUP BY table
ORDER BY table;

DROP TABLE gap1k_none;
DROP TABLE gap1k_bitpacking;
DROP TABLE gap1k_fastpfor;
DROP TABLE gap1k_binarypacking;
DROP TABLE gap1k_optpfor;

-- ============================================================================
-- Scenario 3: Huge uniform gaps (delta = 10000, needs 14 bits)
-- ============================================================================

DROP TABLE IF EXISTS gap10k_none;
DROP TABLE IF EXISTS gap10k_bitpacking;
DROP TABLE IF EXISTS gap10k_fastpfor;
DROP TABLE IF EXISTS gap10k_binarypacking;
DROP TABLE IF EXISTS gap10k_optpfor;

CREATE TABLE gap10k_none
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap10k_bitpacking
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap10k_fastpfor
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'fastpfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap10k_binarypacking
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'binarypacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE gap10k_optpfor
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'optpfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

-- 1000 tokens (word_0..word_999), each appears every 1000th row -> delta = 10000
INSERT INTO gap10k_none SELECT number, concat('word_', toString(number % 1000)) FROM numbers(1000000);
INSERT INTO gap10k_bitpacking SELECT number, concat('word_', toString(number % 1000)) FROM numbers(1000000);
INSERT INTO gap10k_fastpfor SELECT number, concat('word_', toString(number % 1000)) FROM numbers(1000000);
INSERT INTO gap10k_binarypacking SELECT number, concat('word_', toString(number % 1000)) FROM numbers(1000000);
INSERT INTO gap10k_optpfor SELECT number, concat('word_', toString(number % 1000)) FROM numbers(1000000);

OPTIMIZE TABLE gap10k_none FINAL;
OPTIMIZE TABLE gap10k_bitpacking FINAL;
OPTIMIZE TABLE gap10k_fastpfor FINAL;
OPTIMIZE TABLE gap10k_binarypacking FINAL;
OPTIMIZE TABLE gap10k_optpfor FINAL;

SELECT 'Scenario 3: Gap=10000 (1000 tokens, 10K postings each, 14 bits/delta)';

SELECT
    replaceOne(table, 'gap10k_', '') AS codec,
    sum(rows) AS rows,
    sum(secondary_indices_compressed_bytes) AS index_size
FROM system.parts
WHERE database = currentDatabase() AND active AND table LIKE 'gap10k_%'
GROUP BY table
ORDER BY table;

DROP TABLE gap10k_none;
DROP TABLE gap10k_bitpacking;
DROP TABLE gap10k_fastpfor;
DROP TABLE gap10k_binarypacking;
DROP TABLE gap10k_optpfor;
