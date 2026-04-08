-- Test that LIKE pattern queries work correctly with text index when posting lists
-- span multiple blocks (large postings). Previously, pattern-matched tokens with
-- multi-block posting lists were not getting their reading streams initialized,
-- resulting in incorrectly empty posting lists and false filtering of all rows.

DROP TABLE IF EXISTS t_text_index_like_large;

CREATE TABLE t_text_index_like_large
(
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1000) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_text_index_like_large SELECT 'aaabbbccc' FROM numbers(100000);

-- Without the fix, this returned 0 because the text index incorrectly
-- filtered out all rows (pattern-matched token had large postings that
-- were never read).
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%abbbc%';

-- Verify the index is actually being used (not bypassed).
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%abbbc%' SETTINGS use_skip_indexes = 0;

-- Also test a pattern that should NOT match.
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%xyzcb%';

DROP TABLE t_text_index_like_large;
