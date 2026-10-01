-- { echo }
-- Direct read from a text index when every search token has a single posting list block: results match evaluation
-- without the index for dense and sparse ranges of rows, granules crossing and spanning 65536-row ranges, lazy mode,
-- several parts and arrays.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_odd;
DROP TABLE IF EXISTS tab_wide;
DROP TABLE IF EXISTS tab_pfor;
DROP TABLE IF EXISTS tab_parts;
DROP TABLE IF EXISTS tab_arr;
DROP TABLE IF EXISTS tab_blocks;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1048576, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT number, concat(
    if(number % 2 = 0, 'a ', ''),
    if(number % 3 = 0, 'b ', ''),
    if(number % 5 = 0, 'c ', ''),
    if(number BETWEEN 100000 AND 149999, 'r ', ''),
    if(number % 1000 = 10, 'x ', ''),
    if((number < 65536 AND number % 7 = 0) OR (number >= 65536 AND number % 100 = 0), 'y ', ''),
    if(number BETWEEN 70656 AND 71679, 'm ', ''),
    if(number % 50 = 7, 'q ', ''),
    'z') FROM numbers(200000);

SET use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'a');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'x');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'r');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'y');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'm');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'z');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAnyTokens(s, ['x', 'r']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAnyTokens(s, ['m', 'q', 'y']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'b', 'c']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'r']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'absent']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAnyTokens(s, ['absent1', 'absent2']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE NOT hasToken(s, 'a');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'x') OR hasToken(s, 'r');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'c') AND id BETWEEN 30000 AND 30500;
SELECT count(), sum(cityHash64(id)) FROM tab WHERE s LIKE '% x %';
SELECT count(), sum(cityHash64(id)) FROM tab PREWHERE hasToken(s, 'b') WHERE id >= 1000;
SELECT count(), sum(cityHash64(id)) FROM (SELECT id FROM tab WHERE hasToken(s, 'y') ORDER BY id DESC LIMIT 1000000) SETTINGS optimize_read_in_order = 1;
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAnyTokens(s, ['a', 'x']) SETTINGS max_threads = 3, max_block_size = 777, merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 1;
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'c') SETTINGS max_block_size = 1024;

CREATE TABLE tab_odd
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1048576, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3001, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_odd SELECT * FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasToken(s, 'a');
SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasToken(s, 'x');
SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasToken(s, 'r');
SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasToken(s, 'y');
SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasAnyTokens(s, ['m', 'r']);
SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasToken(s, 'a') SETTINGS max_block_size = 3001;
SELECT count(), sum(cityHash64(id)) FROM tab_odd WHERE hasToken(s, 'x') SETTINGS max_block_size = 3001;

CREATE TABLE tab_wide
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1048576, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_wide SELECT * FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_wide WHERE hasToken(s, 'c');
SELECT count(), sum(cityHash64(id)) FROM tab_wide WHERE hasToken(s, 'x');
SELECT count(), sum(cityHash64(id)) FROM tab_wide WHERE hasToken(s, 'y');

CREATE TABLE tab_pfor
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1048576, posting_list_codec = 'pfor')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_pfor SELECT * FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_pfor WHERE hasToken(s, 'a') SETTINGS text_index_posting_list_apply_mode = 'lazy';
SELECT count(), sum(cityHash64(id)) FROM tab_pfor WHERE hasToken(s, 'x') SETTINGS text_index_posting_list_apply_mode = 'lazy';
SELECT count(), sum(cityHash64(id)) FROM tab_pfor WHERE hasToken(s, 'y') SETTINGS text_index_posting_list_apply_mode = 'lazy';
SELECT count(), sum(cityHash64(id)) FROM tab_pfor WHERE hasAnyTokens(s, ['x', 'r']) SETTINGS text_index_posting_list_apply_mode = 'lazy';
SELECT count(), sum(cityHash64(id)) FROM tab_pfor WHERE hasAllTokens(s, ['a', 'b']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

CREATE TABLE tab_parts
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1048576, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab_parts;
INSERT INTO tab_parts SELECT * FROM tab WHERE id < 70000;
INSERT INTO tab_parts SELECT * FROM tab WHERE id >= 70000 AND id < 140000;
INSERT INTO tab_parts SELECT * FROM tab WHERE id >= 140000;

SELECT count(), sum(cityHash64(id)) FROM tab_parts WHERE hasToken(s, 'y') SETTINGS max_threads = 3;
SELECT count(), sum(cityHash64(id)) FROM tab_parts WHERE hasAnyTokens(s, ['x', 'r']) SETTINGS max_threads = 3;

CREATE TABLE tab_arr
(
    id UInt64,
    tags Array(String),
    INDEX idx tags TYPE text(tokenizer = 'array', posting_list_block_size = 1048576, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_arr SELECT id, splitByChar(' ', s) FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_arr WHERE has(tags, 'a');
SELECT count(), sum(cityHash64(id)) FROM tab_arr WHERE hasAny(tags, ['x', 'r']);

CREATE TABLE tab_blocks
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 4096, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_blocks SELECT * FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_blocks WHERE hasToken(s, 'a');
SELECT count(), sum(cityHash64(id)) FROM tab_blocks WHERE hasAnyTokens(s, ['a', 'x']);

-- Rows are filled directly from the posting lists read during the index analysis only when every search token has one
-- posting list block.
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'a')
    SETTINGS use_query_condition_cache = 0, log_comment = 'single_block_postings_tab';
SELECT count(), sum(cityHash64(id)) FROM tab_pfor WHERE hasToken(s, 'a')
    SETTINGS use_query_condition_cache = 0, text_index_posting_list_apply_mode = 'lazy', log_comment = 'single_block_postings_tab_pfor';
SELECT count(), sum(cityHash64(id)) FROM tab_blocks WHERE hasToken(s, 'a')
    SETTINGS use_query_condition_cache = 0, log_comment = 'single_block_postings_tab_blocks';

SYSTEM FLUSH LOGS query_log;

WITH initial_queries AS
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND is_initial_query = 1
      AND log_comment LIKE 'single_block_postings_%'
)
SELECT q.log_comment, sum(ProfileEvents['TextIndexFilledFromFoldedPostings']) > 0 AS filled
FROM system.query_log AS l
INNER JOIN initial_queries AS q ON l.initial_query_id = q.query_id
WHERE l.event_date >= yesterday() AND l.event_time >= now() - 600
  AND l.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE tab;
DROP TABLE tab_odd;
DROP TABLE tab_wide;
DROP TABLE tab_pfor;
DROP TABLE tab_parts;
DROP TABLE tab_arr;
DROP TABLE tab_blocks;
