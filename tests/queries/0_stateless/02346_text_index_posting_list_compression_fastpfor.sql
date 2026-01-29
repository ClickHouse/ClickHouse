-- Tags: no-fasttest
-- ^^ FastPFor codecs require the FastPFor library which is not built in fasttest

-- This test validates the FastPFor family of posting-list codecs:
-- - fastpfor: FastPFor SIMD-accelerated codec
-- - binarypacking: SIMD Binary Packing with fastest decode speed
-- - simple8b: packs multiple small integers into 64-bit words
-- - streamvbyte: byte-aligned variable-byte encoding with SIMD
-- - optpfor: Optimized Patched Frame-of-Reference with highest compression ratio
--
-- The inserted data covers a range of posting-list shapes:
-- - very large lists (aa/bb/cc),
-- - a block-boundary case (129 hits = 128 + tail),
-- - a medium non-aligned size (1003 hits),
-- - a single-hit list, and
-- - very sparse lists (2 and 5 hits).

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

-- ============================================================================
-- Test FastPFor codec
-- ============================================================================

SELECT '-- Test fastpfor codec';

DROP TABLE IF EXISTS tab_fastpfor;
DROP TABLE IF EXISTS tab_baseline;

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

CREATE TABLE tab_baseline
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

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_baseline
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_baseline
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

INSERT INTO tab_baseline
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 16:00:00',
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
FROM numbers(2000);

INSERT INTO tab_baseline
SELECT
    '2026-01-09 16:00:00',
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
FROM numbers(2000);

OPTIMIZE TABLE tab_fastpfor FINAL;
OPTIMIZE TABLE tab_baseline FINAL;

-- Validate results match baseline
SELECT
    (SELECT count() FROM tab_baseline WHERE hasToken(str, 'aa')) AS count_baseline,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'aa')) AS count_fastpfor,
    (count_fastpfor = count_baseline) AS ok_aa;

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'tail129')) AS count_fastpfor,
    (count_fastpfor = 129) AS ok_tail129;

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'mid1003')) AS count_fastpfor,
    (count_fastpfor = 1003) AS ok_mid1003;

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'single')) AS count_fastpfor,
    (count_fastpfor = 1) AS ok_single;

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'rare2')) AS count_fastpfor,
    (count_fastpfor = 2) AS ok_rare2;

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'rare5')) AS count_fastpfor,
    (count_fastpfor = 5) AS ok_rare5;

DROP TABLE tab_fastpfor;
DROP TABLE tab_baseline;

-- ============================================================================
-- Test BinaryPacking codec
-- ============================================================================

SELECT '-- Test binarypacking codec';

DROP TABLE IF EXISTS tab_binarypacking;

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

INSERT INTO tab_binarypacking
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_binarypacking
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_binarypacking
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

OPTIMIZE TABLE tab_binarypacking FINAL;

SELECT
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'aa')) AS count_bp,
    (count_bp = 341334) AS ok_aa;

SELECT
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'tail129')) AS count_bp,
    (count_bp = 129) AS ok_tail129;

SELECT
    (SELECT count() FROM tab_binarypacking WHERE hasToken(str, 'mid1003')) AS count_bp,
    (count_bp = 1003) AS ok_mid1003;

DROP TABLE tab_binarypacking;

-- ============================================================================
-- Test Simple8b codec
-- ============================================================================

SELECT '-- Test simple8b codec';

DROP TABLE IF EXISTS tab_simple8b;

CREATE TABLE tab_simple8b
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'simple8b'
    )
)
ENGINE = MergeTree
ORDER BY ts;

INSERT INTO tab_simple8b
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_simple8b
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_simple8b
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

OPTIMIZE TABLE tab_simple8b FINAL;

SELECT
    (SELECT count() FROM tab_simple8b WHERE hasToken(str, 'aa')) AS count_s8b,
    (count_s8b = 341334) AS ok_aa;

SELECT
    (SELECT count() FROM tab_simple8b WHERE hasToken(str, 'tail129')) AS count_s8b,
    (count_s8b = 129) AS ok_tail129;

SELECT
    (SELECT count() FROM tab_simple8b WHERE hasToken(str, 'mid1003')) AS count_s8b,
    (count_s8b = 1003) AS ok_mid1003;

DROP TABLE tab_simple8b;

-- ============================================================================
-- Test StreamVByte codec
-- ============================================================================

SELECT '-- Test streamvbyte codec';

DROP TABLE IF EXISTS tab_streamvbyte;

CREATE TABLE tab_streamvbyte
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'streamvbyte'
    )
)
ENGINE = MergeTree
ORDER BY ts;

INSERT INTO tab_streamvbyte
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_streamvbyte
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_streamvbyte
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

OPTIMIZE TABLE tab_streamvbyte FINAL;

SELECT
    (SELECT count() FROM tab_streamvbyte WHERE hasToken(str, 'aa')) AS count_svb,
    (count_svb = 341334) AS ok_aa;

SELECT
    (SELECT count() FROM tab_streamvbyte WHERE hasToken(str, 'tail129')) AS count_svb,
    (count_svb = 129) AS ok_tail129;

SELECT
    (SELECT count() FROM tab_streamvbyte WHERE hasToken(str, 'mid1003')) AS count_svb,
    (count_svb = 1003) AS ok_mid1003;

DROP TABLE tab_streamvbyte;

-- ============================================================================
-- Test OptPFor codec
-- ============================================================================

SELECT '-- Test optpfor codec';

DROP TABLE IF EXISTS tab_optpfor;

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

INSERT INTO tab_optpfor
SELECT
    '2026-01-09 10:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

INSERT INTO tab_optpfor
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_optpfor
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

OPTIMIZE TABLE tab_optpfor FINAL;

SELECT
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'aa')) AS count_opt,
    (count_opt = 341334) AS ok_aa;

SELECT
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'tail129')) AS count_opt,
    (count_opt = 129) AS ok_tail129;

SELECT
    (SELECT count() FROM tab_optpfor WHERE hasToken(str, 'mid1003')) AS count_opt,
    (count_opt = 1003) AS ok_mid1003;

DROP TABLE tab_optpfor;

-- ============================================================================
-- Test invalid codec error
-- ============================================================================

SELECT '-- Test invalid codec error';

CREATE TABLE tab_invalid
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'invalid_codec_name'
    )
)
ENGINE = MergeTree
ORDER BY ts; -- { serverError BAD_ARGUMENTS }
