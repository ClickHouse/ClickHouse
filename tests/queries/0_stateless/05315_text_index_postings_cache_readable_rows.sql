-- The same text predicate must return the same rows regardless of the key range searched before it.
-- The shared postings cache used to keep a posting list clipped to the readable rows of an earlier query.
-- https://github.com/ClickHouse/ClickHouse/issues/120951

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_text_index_cache_bitpacking;
DROP TABLE IF EXISTS t_text_index_cache_none;

CREATE TABLE t_text_index_cache_bitpacking
(
    id UInt32,
    k UInt32,
    message String,
    INDEX idx_k(k) TYPE minmax GRANULARITY 1,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 64) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';

CREATE TABLE t_text_index_cache_none
(
    id UInt32,
    k UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'none') GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';

-- 'rare' is in two rows far apart, 'common' spans many posting list blocks.
INSERT INTO t_text_index_cache_bitpacking SELECT number, number, if(number IN (1, 4000), 'rare common', 'common filler') FROM numbers(4096);
INSERT INTO t_text_index_cache_none SELECT * FROM t_text_index_cache_bitpacking;

SELECT 'bitpacking, primary key';
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE id < 128 AND hasAllTokens(message, ['rare', 'common']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE id >= 3000 AND hasAllTokens(message, ['rare', 'common']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE hasAllTokens(message, ['rare', 'common']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE id < 128 AND hasAnyTokens(message, ['rare', 'missing']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE hasAnyTokens(message, ['rare', 'missing']) ORDER BY id);

SELECT 'bitpacking, skip index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE k >= 3000 AND hasAllTokens(message, ['common', 'rare']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE k < 128 AND hasAllTokens(message, ['common', 'rare']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_bitpacking WHERE hasAllTokens(message, ['common', 'rare']) ORDER BY id);

SELECT 'none, primary key';
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_none WHERE id < 128 AND hasAllTokens(message, ['rare', 'common']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_none WHERE id >= 3000 AND hasAllTokens(message, ['rare', 'common']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_index_cache_none WHERE hasAllTokens(message, ['rare', 'common']) ORDER BY id);

DROP TABLE t_text_index_cache_bitpacking;
DROP TABLE t_text_index_cache_none;
