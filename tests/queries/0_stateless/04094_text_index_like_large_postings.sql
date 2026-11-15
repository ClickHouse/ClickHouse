-- Test that LIKE pattern queries work correctly with text index when posting lists
-- span multiple blocks (large postings).

DROP TABLE IF EXISTS t_text_index_like_large;

CREATE TABLE t_text_index_like_large
(
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1000) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_text_index_like_large SELECT 'aaabbbccc' FROM numbers(100000);
OPTIMIZE TABLE t_text_index_like_large;

SELECT token, num_posting_blocks > 1 FROM mergeTreeTextIndex(currentDatabase(), 't_text_index_like_large', 'idx_s');

SELECT count() FROM t_text_index_like_large WHERE s LIKE '%abbbc%';
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%abbbc%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%xyzcb%';

DROP TABLE t_text_index_like_large;
