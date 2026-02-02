-- Tags: no-fasttest
-- ^^ FastPFor codecs require the FastPFor library which is not built in fasttest

-- This test validates all posting-list codecs for text indexes:
-- - none: no compression
-- - bitpacking: differential (delta) coding + bit-packing
-- - fastpfor: FastPFor SIMD-accelerated codec
-- - binarypacking: SIMD Binary Packing with fastest decode speed
-- - optpfor: Optimized Patched Frame-of-Reference with highest compression ratio
--
-- The inserted data covers a range of posting-list shapes:
-- - very large lists (aa/bb/cc),
-- - a block-boundary case (129 hits = 128 + tail),
-- - a medium non-aligned size (1003 hits),
-- - a single-hit list, and
-- - very sparse lists (2 and 5 hits).
-- After OPTIMIZE FINAL to stabilize on-disk parts, each hasToken() query validates that all codec
-- tables return identical counts, ensuring correctness across full blocks, tail blocks, and small-N cases.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

-- Create tables for all codecs

DROP TABLE IF EXISTS tab_uncompressed;
DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS tab_fastpfor;
DROP TABLE IF EXISTS tab_binarypacking;
DROP TABLE IF EXISTS tab_optpfor;

CREATE TABLE tab_uncompressed
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'none'
    )
)
ENGINE = MergeTree
ORDER BY ts;

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
ORDER BY ts;

CREATE TABLE tab_fastpfor
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'fastpfor'
    )
)
ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE tab_binarypacking
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'binarypacking'
    )
)
ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE tab_optpfor
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'optpfor'
    )
)
ENGINE = MergeTree
ORDER BY ts;

-- Insert test data into all tables

-- Large posting lists (aa/bb/cc each ~341334 hits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);
INSERT INTO tab_optpfor SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') FROM numbers(1024000);

-- Block boundary case (129 = 128 + 1 tail) and single hit
INSERT INTO tab_uncompressed SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_bitpacking SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_fastpfor SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_binarypacking SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);
INSERT INTO tab_optpfor SELECT '2026-01-09 12:00:00', multiIf(number < 129, 'tail129', number = 129, 'single', 'noise') FROM numbers(512);

-- Medium non-aligned (1003 hits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_bitpacking SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_fastpfor SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_binarypacking SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);
INSERT INTO tab_optpfor SELECT '2026-01-09 14:00:00', if(number < 1003, 'mid1003', 'noise') FROM numbers(1500);

-- Very sparse lists (2 and 5 hits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);
INSERT INTO tab_optpfor SELECT '2026-01-09 16:00:00', multiIf(number IN (0, 777), 'rare2', number IN (1, 2, 3, 4, 5), 'rare5', 'noise') FROM numbers(2000);

-- Optimize all tables
OPTIMIZE TABLE tab_uncompressed FINAL;
OPTIMIZE TABLE tab_bitpacking FINAL;
OPTIMIZE TABLE tab_fastpfor FINAL;
OPTIMIZE TABLE tab_binarypacking FINAL;
OPTIMIZE TABLE tab_optpfor FINAL;

-- Validate results across all codecs

-- Test 'aa' token (expected 341334 hits) - validates large posting list decoding
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'aa')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'aa')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'aa')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'aa')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'aa')) AS cnt_optpfor,
    cnt_uncompressed = 341334 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_aa;

-- Test 'tail129' token (expected 129 hits) - validates block boundary (128 + 1 tail)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'tail129')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'tail129')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'tail129')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'tail129')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'tail129')) AS cnt_optpfor,
    cnt_uncompressed = 129 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_tail129;

-- Test 'mid1003' token (expected 1003 hits) - validates multi-block + non-trivial tail
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'mid1003')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'mid1003')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'mid1003')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'mid1003')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'mid1003')) AS cnt_optpfor,
    cnt_uncompressed = 1003 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_mid1003;

-- Test 'single' token (expected 1 hit) - validates single-element posting list
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'single')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'single')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'single')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'single')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'single')) AS cnt_optpfor,
    cnt_uncompressed = 1 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_single;

-- Test 'rare2' token (expected 2 hits) - validates very sparse posting list
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'rare2')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'rare2')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'rare2')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'rare2')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'rare2')) AS cnt_optpfor,
    cnt_uncompressed = 2 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_rare2;

-- Test 'rare5' token (expected 5 hits) - validates small-N posting list
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'rare5')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'rare5')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'rare5')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'rare5')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'rare5')) AS cnt_optpfor,
    cnt_uncompressed = 5 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_rare5;

-- ============================================================================
-- Additional Edge Cases
-- ============================================================================

-- Exact block boundaries (128, 256) and near-boundaries (127, 255, 257)
INSERT INTO tab_uncompressed SELECT '2026-01-09 18:00:00', multiIf(number < 128, 'exact128', number < 255, 'near127', number < 512, 'near257', 'noise') FROM numbers(1000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 18:00:00', multiIf(number < 128, 'exact128', number < 255, 'near127', number < 512, 'near257', 'noise') FROM numbers(1000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 18:00:00', multiIf(number < 128, 'exact128', number < 255, 'near127', number < 512, 'near257', 'noise') FROM numbers(1000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 18:00:00', multiIf(number < 128, 'exact128', number < 255, 'near127', number < 512, 'near257', 'noise') FROM numbers(1000);
INSERT INTO tab_optpfor SELECT '2026-01-09 18:00:00', multiIf(number < 128, 'exact128', number < 255, 'near127', number < 512, 'near257', 'noise') FROM numbers(1000);

-- Consecutive row IDs (delta=1, minimal bits)
INSERT INTO tab_uncompressed SELECT '2026-01-09 20:00:00', 'consecutive' FROM numbers(5000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 20:00:00', 'consecutive' FROM numbers(5000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 20:00:00', 'consecutive' FROM numbers(5000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 20:00:00', 'consecutive' FROM numbers(5000);
INSERT INTO tab_optpfor SELECT '2026-01-09 20:00:00', 'consecutive' FROM numbers(5000);

-- Multiple tokens per row
INSERT INTO tab_uncompressed SELECT '2026-01-09 22:00:00', concat('alpha beta gamma ', toString(number % 50)) FROM numbers(2000);
INSERT INTO tab_bitpacking SELECT '2026-01-09 22:00:00', concat('alpha beta gamma ', toString(number % 50)) FROM numbers(2000);
INSERT INTO tab_fastpfor SELECT '2026-01-09 22:00:00', concat('alpha beta gamma ', toString(number % 50)) FROM numbers(2000);
INSERT INTO tab_binarypacking SELECT '2026-01-09 22:00:00', concat('alpha beta gamma ', toString(number % 50)) FROM numbers(2000);
INSERT INTO tab_optpfor SELECT '2026-01-09 22:00:00', concat('alpha beta gamma ', toString(number % 50)) FROM numbers(2000);

OPTIMIZE TABLE tab_uncompressed FINAL;
OPTIMIZE TABLE tab_bitpacking FINAL;
OPTIMIZE TABLE tab_fastpfor FINAL;
OPTIMIZE TABLE tab_binarypacking FINAL;
OPTIMIZE TABLE tab_optpfor FINAL;

-- Test 'exact128' (128 postings, exactly 1 full block)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'exact128')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'exact128')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'exact128')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'exact128')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'exact128')) AS cnt_optpfor,
    cnt_uncompressed = 128 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_exact128;

-- Test 'near127' (127 postings, tail only)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'near127')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'near127')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'near127')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'near127')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'near127')) AS cnt_optpfor,
    cnt_uncompressed = 127 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_near127;

-- Test 'near257' (257 postings, 2 blocks + 1 tail)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'near257')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'near257')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'near257')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'near257')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'near257')) AS cnt_optpfor,
    cnt_uncompressed = 257 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_near257;

-- Test 'consecutive' (5000 postings, delta=1)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'consecutive')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'consecutive')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'consecutive')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'consecutive')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'consecutive')) AS cnt_optpfor,
    cnt_uncompressed = 5000 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_consecutive;

-- Test 'alpha' (multi-token, 2000 postings)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'alpha')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'alpha')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'alpha')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'alpha')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'alpha')) AS cnt_optpfor,
    cnt_uncompressed = 2000 AND cnt_bitpacking = cnt_uncompressed AND cnt_fastpfor = cnt_uncompressed
        AND cnt_binarypacking = cnt_uncompressed AND cnt_optpfor = cnt_uncompressed AS ok_alpha;

-- Test nonexistent token (empty result)
SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'nonexistent')) AS cnt_uncompressed,
    (SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'nonexistent')) AS cnt_bitpacking,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'nonexistent')) AS cnt_fastpfor,
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'nonexistent')) AS cnt_binarypacking,
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'nonexistent')) AS cnt_optpfor,
    cnt_uncompressed = 0 AND cnt_bitpacking = 0 AND cnt_fastpfor = 0
        AND cnt_binarypacking = 0 AND cnt_optpfor = 0 AS ok_nonexistent;

-- Cleanup

DROP TABLE tab_uncompressed;
DROP TABLE tab_bitpacking;
DROP TABLE tab_fastpfor;
DROP TABLE tab_binarypacking;
DROP TABLE tab_optpfor;
