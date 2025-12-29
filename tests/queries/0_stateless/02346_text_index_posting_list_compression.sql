-- This test compares two identical tables that differ only in the text index posting-list compression setting (enable_posting_list_compression=true vs false).
-- The inserted data covers a range of posting-list shapes: very large lists (aa/bb/cc),
-- a block-boundary case (129 hits = 128 + tail), a medium non-aligned size (1003 hits), a single-hit list, and very sparse lists (2 and 5 hits).
-- After OPTIMIZE FINAL to stabilize on-disk parts, each hasToken() query validates that the compressed
-- and uncompressed tables return identical counts (and matches expected exact counts for key tokens),
-- ensuring correctness across full blocks, tail blocks, and small-N cases.

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_comp;

CREATE TABLE tab_comp
(
    _ts DateTime,
    _content String,
    INDEX inv_idx _content TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'simdcomp'
    )
)
ENGINE = MergeTree
ORDER BY _ts;

DROP TABLE IF EXISTS tab_nocomp;

CREATE TABLE tab_nocomp
(
    _ts DateTime,
    _content String,
    INDEX inv_idx _content TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY _ts;

INSERT INTO tab_comp
SELECT
    now() AS _ts,
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS _content
FROM numbers(1024000);

INSERT INTO tab_nocomp
SELECT
    now() AS _ts,
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS _content
FROM numbers(1024000);

INSERT INTO tab_comp
SELECT
    now() AS _ts,
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS _content
FROM numbers(512);

INSERT INTO tab_nocomp
SELECT
    now() AS _ts,
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS _content
FROM numbers(512);

INSERT INTO tab_comp
SELECT
    now() AS _ts,
    if(number < 1003, 'mid1003', 'noise') AS _content
FROM numbers(1500);

INSERT INTO tab_nocomp
SELECT
    now() AS _ts,
    if(number < 1003, 'mid1003', 'noise') AS _content
FROM numbers(1500);

INSERT INTO tab_comp
SELECT
    now() AS _ts,
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS _content
FROM numbers(2000);

INSERT INTO tab_nocomp
SELECT
    now() AS _ts,
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS _content
FROM numbers(2000);

OPTIMIZE TABLE tab_comp FINAL;

OPTIMIZE TABLE tab_nocomp FINAL;

-- Validates that a very large/high-frequency posting list is decoded correctly by checking the count in the compressed table matches the uncompressed baseline.

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'aa')) AS comp_aa,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'aa')) AS nocomp_aa,
    (comp_aa = nocomp_aa) AS ok_aa;

-- Tests the block-boundary case (expected 129 hits, i.e. one full 128-value block plus a 1-value tail) and verifies compressed vs uncompressed counts are identical.

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'tail129')) AS comp_tail129,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'tail129')) AS nocomp_tail129,
    comp_tail129, nocomp_tail129,
    (comp_tail129 = 129) AS ok_tail129,
    (comp_tail129 = nocomp_tail129) AS ok_tail129_eq;

-- Tests a medium-length, non-aligned posting list (expected 1003 hits) to cover multi-block + non-trivial tail handling; also checks equality with the uncompressed table.

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'mid1003')) AS comp_mid1003,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'mid1003')) AS nocomp_mid1003,
    (comp_mid1003 = 1003) AS ok_mid1003,
    (comp_mid1003 = nocomp_mid1003) AS ok_mid1003_eq;

-- Tests the single-element posting list case (expected 1 hit) and ensures compressed and uncompressed results match.

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'single')) AS comp_single,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'single')) AS nocomp_single,
    (comp_single = 1) AS ok_single,
    (comp_single = nocomp_single) AS ok_single_eq;

-- Tests a very small/sparse posting list (expected 2 hits) and checks compressed equals uncompressed.

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'rare2')) AS comp_rare2,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'rare2')) AS nocomp_rare2,
    (comp_rare2 = 2) AS ok_rare2,
    (comp_rare2 = nocomp_rare2) AS ok_rare2_eq;

-- Tests another small-N posting list (expected 5 hits) to cover additional short-list behavior; also checks compressed equals uncompressed.

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'rare5')) AS comp_rare5,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'rare5')) AS nocomp_rare5,
    (comp_rare5 = 5) AS ok_rare5,
    (comp_rare5 = nocomp_rare5) AS ok_rare5_eq;

DROP TABLE tab_comp;

DROP TABLE tab_nocomp;
