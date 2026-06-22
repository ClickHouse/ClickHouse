-- Tags: no-fasttest
-- no-fasttest: the FastPFOR library is not built in the fast test (ENABLE_LIBRARIES=0), so the
-- 'fastpfor' posting list codec is unavailable there.

-- This test validates the FastPFOR posting-list codec (posting_list_codec = 'fastpfor') by comparing a
-- table that uses it against an otherwise identical uncompressed table. The inserted data covers a range
-- of posting-list shapes:
-- - very large lists (aa/bb/cc),
-- - a block-boundary case (129 hits = 128 + tail),
-- - a medium non-aligned size (1003 hits),
-- - a single-hit list, and
-- - very sparse lists (2 and 5 hits).
-- After OPTIMIZE FINAL each hasToken() query checks that the FastPFOR-compressed table returns the same
-- counts as the uncompressed baseline (and the expected exact counts for key tokens), exercising full
-- blocks, tail blocks, and small-N cases through both the eager decoder and the lazy posting cursor.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_fastpfor;
DROP TABLE IF EXISTS tab_uncompressed;

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

CREATE TABLE tab_uncompressed
(
    ts DateTime,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha'
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
FROM numbers(10000);

INSERT INTO tab_uncompressed
SELECT
    '2026-01-09 11:00:00',
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(10000);

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 12:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_uncompressed
SELECT
    '2026-01-09 13:00:00',
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
FROM numbers(512);

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 14:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

INSERT INTO tab_uncompressed
SELECT
    '2026-01-09 15:00:00',
    if(number < 1003, 'mid1003', 'noise') AS str
FROM numbers(1500);

INSERT INTO tab_fastpfor
SELECT
    '2026-01-09 16:00:00',
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
FROM numbers(2000);

INSERT INTO tab_uncompressed
SELECT
    '2026-01-09 17:00:00',
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
FROM numbers(2000);

OPTIMIZE TABLE tab_fastpfor FINAL;
OPTIMIZE TABLE tab_uncompressed FINAL;

-- Validates that a very large/high-frequency posting list is decoded correctly by checking the count in the FastPFOR table matches the uncompressed baseline.

SELECT
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'aa')) AS count_uncompressed,
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'aa')) AS count_fastpfor,
    (count_fastpfor = count_uncompressed) AS ok_aa;

-- Tests the block-boundary case (expected 129 hits, i.e. one full 128-value block plus a 1-value tail) and verifies FastPFOR vs uncompressed counts are identical.

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'tail129')) AS count_fastpfor,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'tail129')) AS count_uncompressed,
    (count_fastpfor = 129) AS ok_tail129,
    (count_fastpfor = count_uncompressed) AS ok_tail129_eq;

-- Tests a medium-length, non-aligned posting list (expected 1003 hits) to cover multi-block + non-trivial tail handling; also checks equality with the uncompressed table.

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'mid1003')) AS count_fastpfor,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'mid1003')) AS count_uncompressed,
    (count_fastpfor = 1003) AS ok_mid1003,
    (count_fastpfor = count_uncompressed) AS ok_mid1003_eq;

-- Tests the single-element posting list case (expected 1 hit) and ensures FastPFOR and uncompressed results match.

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'single')) AS count_fastpfor,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'single')) AS count_uncompressed,
    (count_fastpfor = 1) AS ok_single,
    (count_fastpfor = count_uncompressed) AS ok_single_eq;

-- Tests a very small/sparse posting list (expected 2 hits) and checks FastPFOR equals uncompressed.

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'rare2')) AS count_fastpfor,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'rare2')) AS count_uncompressed,
    (count_fastpfor = 2) AS ok_rare2,
    (count_fastpfor = count_uncompressed) AS ok_rare2_eq;

-- Tests another small-N posting list (expected 5 hits) to cover additional short-list behavior; also checks FastPFOR equals uncompressed.

SELECT
    (SELECT count() FROM tab_fastpfor WHERE hasToken(str, 'rare5')) AS count_fastpfor,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'rare5')) AS count_uncompressed,
    (count_fastpfor = 5) AS ok_rare5,
    (count_fastpfor = count_uncompressed) AS ok_rare5_eq;

DROP TABLE tab_fastpfor;
DROP TABLE tab_uncompressed;
