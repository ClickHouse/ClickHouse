-- Tags: no-parallel-replicas
-- Trivial count from a text index with compressed multi-segment posting lists: the count is computed
-- with posting-list cursors and must match the count of the normal read path.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET text_index_posting_list_apply_mode = 'lazy';

DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS tab_pfor;

CREATE TABLE tab_bitpacking
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1000)
)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_pfor
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', posting_list_block_size = 1000)
)
ENGINE = MergeTree ORDER BY id;

-- `every` is in every row (dense segments), `half` / `third` / `seventh` are periodic,
-- `head` covers only the first rows, `tail` only the last ones, `rare` fits in a single segment,
-- `burst` is in two short row ranges far apart, so the windows between them have no matches.
INSERT INTO tab_bitpacking SELECT number, concat(
    'every',
    if(number % 2 = 0, ' half', ''),
    if(number % 3 = 0, ' third', ''),
    if(number % 7 = 0, ' seventh', ''),
    if(number < 30000, ' head', ''),
    if(number >= 170000, ' tail', ''),
    if(number % 1000 = 0, ' rare', ''),
    if(number % 150000 < 2000, ' burst', ''))
FROM numbers(200000);

INSERT INTO tab_pfor SELECT * FROM tab_bitpacking;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third'])) WHERE explain LIKE '%ReadFromTextIndexCount%';

SELECT 'tab_bitpacking';
SELECT ['half', 'third'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['half', 'third']);
SELECT ['half', 'third'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third']);
SELECT ['half', 'third'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['half', 'third'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['half', 'seventh'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['half', 'seventh']);
SELECT ['half', 'seventh'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'seventh']);
SELECT ['half', 'seventh'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'seventh']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['half', 'seventh'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'seventh']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['every', 'third'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['every', 'third']);
SELECT ['every', 'third'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['every', 'third']);
SELECT ['every', 'third'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['every', 'third']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['every', 'third'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['every', 'third']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['head', 'tail'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['head', 'tail']);
SELECT ['head', 'tail'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['head', 'tail']);
SELECT ['head', 'tail'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['head', 'tail']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['head', 'tail'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['head', 'tail']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['head', 'half', 'seventh'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['head', 'half', 'seventh']);
SELECT ['head', 'half', 'seventh'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['head', 'half', 'seventh']);
SELECT ['head', 'half', 'seventh'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['head', 'half', 'seventh']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['head', 'half', 'seventh'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['head', 'half', 'seventh']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['tail', 'rare'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['tail', 'rare']);
SELECT ['tail', 'rare'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['tail', 'rare']);
SELECT ['tail', 'rare'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['tail', 'rare']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['tail', 'rare'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['tail', 'rare']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['half', 'rare'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['half', 'rare']);
SELECT ['half', 'rare'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'rare']);
SELECT ['half', 'rare'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'rare']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['half', 'rare'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'rare']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['every', 'half', 'third', 'seventh'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['every', 'half', 'third', 'seventh']);
SELECT ['every', 'half', 'third', 'seventh'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh']);
SELECT ['every', 'half', 'third', 'seventh'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['every', 'half', 'third', 'seventh'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['third', 'missing'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['third', 'missing']);
SELECT ['third', 'missing'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['third', 'missing']);
SELECT ['third', 'missing'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['third', 'missing']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['third', 'missing'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['third', 'missing']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';

SELECT ['burst', 'third'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['burst', 'third']);
SELECT ['burst', 'third'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['burst', 'third']);
SELECT ['burst', 'third'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['burst', 'third']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['burst', 'third'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['burst', 'third']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';
SELECT ['burst', 'every'] AS tokens, 'any', count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['burst', 'every']);
SELECT ['burst', 'every'] AS tokens, 'all', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['burst', 'every']);
SELECT ['burst', 'every'] AS tokens, 'all leapfrog', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['burst', 'every']) SETTINGS text_index_postings_intersection_algorithm = 'leapfrog';
SELECT ['burst', 'every'] AS tokens, 'all bruteforce', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['burst', 'every']) SETTINGS text_index_postings_intersection_algorithm = 'bruteforce';

SELECT 'tab_pfor';
SELECT ['half', 'third'] AS tokens, 'any', count() FROM tab_pfor WHERE hasAnyTokens(text, ['half', 'third']);
SELECT ['half', 'third'] AS tokens, 'all', count() FROM tab_pfor WHERE hasAllTokens(text, ['half', 'third']);
SELECT ['head', 'tail'] AS tokens, 'any', count() FROM tab_pfor WHERE hasAnyTokens(text, ['head', 'tail']);
SELECT ['head', 'tail'] AS tokens, 'all', count() FROM tab_pfor WHERE hasAllTokens(text, ['head', 'tail']);
SELECT ['tail', 'rare'] AS tokens, 'any', count() FROM tab_pfor WHERE hasAnyTokens(text, ['tail', 'rare']);
SELECT ['tail', 'rare'] AS tokens, 'all', count() FROM tab_pfor WHERE hasAllTokens(text, ['tail', 'rare']);
SELECT ['every', 'half', 'third', 'seventh'] AS tokens, 'any', count() FROM tab_pfor WHERE hasAnyTokens(text, ['every', 'half', 'third', 'seventh']);
SELECT ['every', 'half', 'third', 'seventh'] AS tokens, 'all', count() FROM tab_pfor WHERE hasAllTokens(text, ['every', 'half', 'third', 'seventh']);

SELECT 'materialize apply mode';
SELECT count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_bitpacking WHERE hasAnyTokens(text, ['head', 'tail']) SETTINGS text_index_posting_list_apply_mode = 'materialize';

SELECT 'cursors are used only in lazy apply mode';
SELECT 'probe lazy', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third']) FORMAT Null;
SELECT 'probe materialize', count() FROM tab_bitpacking WHERE hasAllTokens(text, ['half', 'third']) SETTINGS text_index_posting_list_apply_mode = 'materialize' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT extract(query, 'probe [a-z]+') AS probe, ProfileEvents['TextIndexLazySegmentsPrepared'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT \'probe %'
ORDER BY probe;

DROP TABLE tab_bitpacking;
DROP TABLE tab_pfor;
