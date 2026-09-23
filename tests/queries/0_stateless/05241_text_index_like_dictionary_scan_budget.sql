-- Tags: no-parallel-replicas
-- no-parallel-replicas: the assertions below read ProfileEvents of the initiator query.

-- A LIKE/ILIKE pattern search over a text index has to match every token of the dictionary of every part
-- it reads, unless the needle starts with a literal prefix that narrows the search to part of it.
-- This test covers the per-part limit on how many of those tokens it may match,
-- text_index_like_max_dictionary_tokens_to_scan: a part that reaches the limit stops using the index for
-- the pattern and evaluates the pattern on its own rows instead. Every query must therefore return the
-- same rows whether the limit is reached or not, and the same rows as the query without the index.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET text_index_like_max_postings_to_read = 1000000;
SET use_query_condition_cache = 0;
SET optimize_rewrite_like_perfect_affix = 0;
SET max_threads = 1;
SET log_queries = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS tab;

-- 1024 rows of four tokens each, all of them distinct, give 4096 dictionary tokens in 64 blocks of 64
-- tokens, and 16 granules of 64 rows. Every needle below matches the single row 999, so a search that
-- completes leaves one granule to read, and one that stops early leaves all of them.
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 64) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO tab SELECT number, concat('aw', toString(number), ' bw', toString(number), ' cw', toString(number), ' dw', toString(number)) FROM numbers(1024);
OPTIMIZE TABLE tab FINAL;

-- An infix needle has no token range to narrow the search, so it visits the whole dictionary. This is the
-- shape the limit is meant for, and the one where the index answers the predicate on its own.
SELECT 'infix, limit reached', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 100, log_comment = 'scan_limit_infix_limited';
SELECT 'infix, no limit', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 0, log_comment = 'scan_limit_infix_unlimited';
-- A limit equal to the number of tokens in the dictionary is not reached.
SELECT 'infix, limit equal to the dictionary', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 4096, log_comment = 'scan_limit_infix_exact';
SELECT 'infix, without the index', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%aw999%' SETTINGS use_skip_indexes = 0;

-- A needle with a literal prefix is narrowed to the blocks that can hold it, so the limit has to be
-- smaller here; a needle anchored only at the end is not narrowed.
SELECT 'prefix, limit reached', arraySort(groupArray(id)) FROM tab WHERE message LIKE 'aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_prefix_limited';
SELECT 'prefix, no limit', arraySort(groupArray(id)) FROM tab WHERE message LIKE 'aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 0, log_comment = 'scan_limit_prefix_unlimited';
SELECT 'prefix, without the index', arraySort(groupArray(id)) FROM tab WHERE message LIKE 'aw999%' SETTINGS use_skip_indexes = 0;
SELECT 'suffix, limit reached', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%dw999' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_suffix_limited';
SELECT 'suffix, without the index', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%dw999' SETTINGS use_skip_indexes = 0;
SELECT 'startsWith, limit reached', arraySort(groupArray(id)) FROM tab WHERE startsWith(message, 'aw999') SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_starts_with_limited';
SELECT 'endsWith, limit reached', arraySort(groupArray(id)) FROM tab WHERE endsWith(message, 'dw999') SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_ends_with_limited';
-- No ILIKE needle here may contain 'k' or 'K': such a needle is refused before the search starts.
SELECT 'ilike infix, limit reached', arraySort(groupArray(id)) FROM tab WHERE message ILIKE '%AW999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_ilike_limited';

-- Without direct reading, the same search is used to skip granules instead of answering the predicate.
SELECT 'granule skipping, limit reached', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%aw999%' SETTINGS query_plan_direct_read_from_text_index = 0, text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_no_direct_read_limited';
SELECT 'granule skipping, no limit', arraySort(groupArray(id)) FROM tab WHERE message LIKE '%aw999%' SETTINGS query_plan_direct_read_from_text_index = 0, text_index_like_max_dictionary_tokens_to_scan = 0, log_comment = 'scan_limit_no_direct_read_unlimited';

-- The limit applies to the pattern only: a token predicate in the same query keeps using the index, which
-- is why this query still skips granules while the pattern reaches the limit.
SELECT 'token and pattern, limit reached', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'bw999') AND message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_token_and_pattern_limited';
SELECT 'token and pattern, without the index', arraySort(groupArray(id)) FROM tab WHERE hasToken(message, 'bw999') AND message LIKE '%aw999%' SETTINGS use_skip_indexes = 0;

SELECT 'Dictionary block size from the table setting';

DROP TABLE IF EXISTS tab_table_block_size;

-- This table takes the dictionary block size from the table setting rather than from an index
-- argument, which is the only way the setting below can still be changed after a part is written.
CREATE TABLE tab_table_block_size
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, text_index_dictionary_block_size = 64;

INSERT INTO tab_table_block_size SELECT number, concat('aw', toString(number), ' bw', toString(number), ' cw', toString(number), ' dw', toString(number)) FROM numbers(1024);
OPTIMIZE TABLE tab_table_block_size FINAL;

-- The part holds 4096 tokens, so a limit of 10000 is never reached and the search prunes. Raising the
-- table setting 128x does not rewrite the part, so the search must still reach the same decision: a
-- limit measured against the table setting instead of the tokens really matched would stop the search
-- after the ALTER and not before it.
SELECT 'table block size, limit not reached, before the alter', arraySort(groupArray(id)) FROM tab_table_block_size WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10000, log_comment = 'scan_limit_table_block_size_before_alter';
ALTER TABLE tab_table_block_size MODIFY SETTING text_index_dictionary_block_size = 8192;
SELECT 'table block size, limit not reached, after the alter', arraySort(groupArray(id)) FROM tab_table_block_size WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10000, log_comment = 'scan_limit_table_block_size_after_alter';
-- A limit below the dictionary still stops the search on the same part.
SELECT 'table block size, limit reached, after the alter', arraySort(groupArray(id)) FROM tab_table_block_size WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 100, log_comment = 'scan_limit_table_block_size_limited';

SELECT 'Array tokenizer';

DROP TABLE IF EXISTS tab_array;

CREATE TABLE tab_array
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 64) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO tab_array SELECT number, concat('aw', toString(number)) FROM numbers(1024);
OPTIMIZE TABLE tab_array FINAL;

SELECT 'array, limit reached', arraySort(groupArray(id)) FROM tab_array WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_array_limited';
SELECT 'array, no limit', arraySort(groupArray(id)) FROM tab_array WHERE message LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 0, log_comment = 'scan_limit_array_unlimited';
SELECT 'array, without the index', arraySort(groupArray(id)) FROM tab_array WHERE message LIKE '%aw999%' SETTINGS use_skip_indexes = 0;

SELECT 'Index over an expression';

DROP TABLE IF EXISTS tab_expression;

CREATE TABLE tab_expression
(
    id UInt32,
    s1 String,
    s2 String,
    INDEX idx(concat(s1, ' ', s2)) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 64) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO tab_expression SELECT number, concat('aw', toString(number)), concat('bw', toString(number)) FROM numbers(1024);
OPTIMIZE TABLE tab_expression FINAL;

SELECT 'expression, limit reached', arraySort(groupArray(id)) FROM tab_expression WHERE concat(s1, ' ', s2) LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 10, log_comment = 'scan_limit_expression_limited';
SELECT 'expression, no limit', arraySort(groupArray(id)) FROM tab_expression WHERE concat(s1, ' ', s2) LIKE '%aw999%' SETTINGS text_index_like_max_dictionary_tokens_to_scan = 0, log_comment = 'scan_limit_expression_unlimited';
SELECT 'expression, without the index', arraySort(groupArray(id)) FROM tab_expression WHERE concat(s1, ' ', s2) LIKE '%aw999%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

-- For every query above: whether the limit stopped the dictionary search, how many dictionary blocks the
-- query read, and whether granules were skipped. All three tables hold 1024 rows.
SELECT
    log_comment,
    ProfileEvents['TextIndexDiscardPatternScanByTokenBudget'] > 0 AS search_stopped_by_the_limit,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    read_rows < 1024 AS granules_skipped
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment LIKE 'scan_limit_%'
ORDER BY log_comment;

DROP TABLE tab;
DROP TABLE tab_table_block_size;
DROP TABLE tab_array;
DROP TABLE tab_expression;
