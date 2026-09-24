-- Intersections of 256 or more posting lists use leapfrog even with `text_index_postings_intersection_algorithm = 'bruteforce'`.
-- Odd rows contain none of the tokens and must not match.

DROP TABLE IF EXISTS tab_256_tokens;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET text_index_posting_list_apply_mode = 'lazy';

DROP TABLE IF EXISTS tab_256_tokens;

CREATE TABLE tab_256_tokens
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_256_tokens SELECT number, if(number % 2 = 0, arrayStringConcat(range(256), ' '), 'none') FROM numbers(1000)
SETTINGS max_insert_threads = 1;

-- Every token spans several posting blocks, so each one gets its own lazy cursor.
SELECT uniqExact(part_name), countIf(num_posting_blocks > 1 AND has_compressed_postings)
FROM mergeTreeTextIndex(currentDatabase(), tab_256_tokens, idx) WHERE token != 'none';

SELECT 'no index', count(), sum(id) FROM tab_256_tokens WHERE hasAllTokens(s, arrayStringConcat(range(256), ' ')) SETTINGS use_skip_indexes = 0;

SELECT '255 bruteforce', count(), sum(id) FROM tab_256_tokens WHERE hasAllTokens(s, arrayStringConcat(range(255), ' '))
SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';

SELECT '256 bruteforce', count(), sum(id) FROM tab_256_tokens WHERE hasAllTokens(s, arrayStringConcat(range(256), ' '))
SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';

SELECT '256 auto', count(), sum(id) FROM tab_256_tokens WHERE hasAllTokens(s, arrayStringConcat(range(256), ' '))
SETTINGS text_index_postings_intersection_algorithm = 'auto';

DROP TABLE tab_256_tokens;
