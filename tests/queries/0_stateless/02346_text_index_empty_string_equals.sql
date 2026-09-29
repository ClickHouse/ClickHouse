-- Tests that `col = ''` on a text-indexed column does not use the text index, regardless of
-- `optimize_empty_string_comparisons` (needed for consistency).

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    col String,
    INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'v1.0 release notes'), (2, 'beta version'), (3, '');

SELECT '-- Must not use the index with and without the optimization';
SELECT * FROM tab WHERE col = '' ORDER BY id SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices='idx'; -- { serverError INDEX_NOT_USED }
SELECT * FROM tab WHERE col = '' ORDER BY id SETTINGS optimize_empty_string_comparisons = 1, force_data_skipping_indices='idx'; -- { serverError INDEX_NOT_USED }

SELECT '-- Sanity check: a non-empty needle still uses the index';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM tab WHERE col = 'beta version' SETTINGS optimize_empty_string_comparisons = 0) WHERE explain ILIKE '%idx%';

DROP TABLE tab;
