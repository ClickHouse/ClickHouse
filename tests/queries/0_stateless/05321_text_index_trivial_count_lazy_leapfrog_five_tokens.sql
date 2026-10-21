-- Trivial count from a text index with five compressed posting lists (N-way leapfrog) whose matches span several count windows.

DROP TABLE IF EXISTS tab_five_tokens;
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET make_distributed_plan = 0;
SET enable_parallel_replicas = 0;
SET serialize_query_plan = 0;
SET text_index_posting_list_apply_mode = 'lazy';

DROP TABLE IF EXISTS tab_five_tokens;

CREATE TABLE tab_five_tokens
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1000)
)
ENGINE = MergeTree ORDER BY id;

-- `rare` fits in a single block and is folded, `burst` is in two short row ranges far apart.
INSERT INTO tab_five_tokens SELECT number, concat(
    'every',
    if(number % 2 = 0, ' half', ''),
    if(number % 3 = 0, ' third', ''),
    if(number % 7 = 0, ' seventh', ''),
    if(number % 1000 = 0, ' rare', ''),
    if(number % 150000 < 2000, ' burst', ''))
FROM numbers(200000)
SETTINGS max_insert_threads = 1;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_five_tokens WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh', 'burst'])) WHERE explain LIKE '%ReadFromTextIndexCount%';

SELECT 'burst leapfrog', count() FROM tab_five_tokens WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh', 'burst']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT 'burst bruteforce', count() FROM tab_five_tokens WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh', 'burst']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT 'rare auto', count() FROM tab_five_tokens WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh', 'rare']) SETTINGS text_index_postings_intersection_algorithm = 'auto';
SELECT 'rare bruteforce', count() FROM tab_five_tokens WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh', 'rare']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';

DROP TABLE tab_five_tokens;
