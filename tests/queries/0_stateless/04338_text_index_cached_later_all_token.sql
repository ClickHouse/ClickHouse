SET enable_full_text_index = 1;
SET use_text_index_postings_cache = 1;

DROP TABLE IF EXISTS tab_text_index_cached_later_all_token;

CREATE TABLE tab_text_index_cached_later_all_token
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 10, index_granularity_bytes = '10Mi';

INSERT INTO tab_text_index_cached_later_all_token
SELECT
    number,
    multiIf(
        number % 10 = 5 AND number >= 15 AND number < 2010, 'a',
        number = 0 OR (number >= 10000 AND number < 10199), 'b',
        'x')
FROM numbers(10200);

OPTIMIZE TABLE tab_text_index_cached_later_all_token FINAL;

SELECT token, cardinality, num_posting_blocks
FROM mergeTreeTextIndex(currentDatabase(), tab_text_index_cached_later_all_token, idx_s)
WHERE token IN ('a', 'b')
ORDER BY token
SETTINGS max_rows_to_read = 8;

SELECT count()
FROM tab_text_index_cached_later_all_token
WHERE hasToken(s, 'b')
SETTINGS text_index_max_cardinality_per_token_for_analysis = 0
FORMAT Null;

SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM tab_text_index_cached_later_all_token
    WHERE hasAllTokens(s, ['a', 'b'])
    SETTINGS text_index_max_cardinality_per_token_for_analysis = 0
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

DROP TABLE tab_text_index_cached_later_all_token;
