-- Tags: no-parallel-replicas

-- The postings the analyzer folds for a query are clipped to the rows the primary key left readable.
-- A later query with the same predicate on the same part must not get them from the server-wide cache.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET text_index_posting_list_apply_mode = 'lazy';
SET use_text_index_postings_cache = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 64) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128;

-- 'rare' is folded by the analyzer; 'common' and 'other' span several posting list blocks and are read lazily.
INSERT INTO tab SELECT number, multiIf(number IN (1, 4000), 'rare common', number BETWEEN 2000 AND 2100, 'other common', 'common filler') FROM numbers(4096);

SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['rare', 'common']) SETTINGS use_skip_indexes = 0;

SELECT groupArray(id) FROM tab WHERE id < 128 AND hasAllTokens(message, ['rare', 'common']);
SELECT groupArray(id) FROM tab WHERE id >= 3000 AND hasAllTokens(message, ['rare', 'common']);
SELECT groupArray(id) FROM tab WHERE hasAllTokens(message, ['rare', 'common']);

SELECT count() FROM tab WHERE id < 2050 AND hasAnyTokens(message, ['rare', 'other']);
SELECT count() FROM tab WHERE hasAnyTokens(message, ['rare', 'other']);

DROP TABLE tab;
