-- { echo }
-- Direct read from a text index matches evaluation without the index for multi-token searches whose
-- posting lists span several posting list blocks and several granules.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_parts;
DROP TABLE IF EXISTS tab_wide;
DROP TABLE IF EXISTS tab_mixed;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 4096, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024;

INSERT INTO tab SELECT number, concat(if(number % 2 = 0, 'a ', ''), if(number % 3 = 0, 'b ', ''), if(number % 5 = 0, 'c ', ''), if(number BETWEEN 100000 AND 149999, 'r ', ''), if(number % 1000 = 10, 'x ', ''), 'z') FROM numbers(200000);

SET use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'b']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'b', 'c']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'b', 'c', 'r', 'z']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'x']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['r', 'b']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['b', 'c']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'r');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasToken(s, 'c');
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAnyTokens(s, ['c', 'r', 'x']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE NOT hasAllTokens(s, ['a', 'b']);
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'c']) AND id BETWEEN 30000 AND 30500;
SELECT count(), sum(cityHash64(id)) FROM (SELECT id FROM tab WHERE hasAllTokens(s, ['a', 'b']) ORDER BY id DESC LIMIT 1000000) SETTINGS optimize_read_in_order = 1;
SELECT count(), sum(cityHash64(id)) FROM tab WHERE hasAllTokens(s, ['a', 'b', 'c']) SETTINGS max_threads = 3, max_block_size = 777, merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 1;

CREATE TABLE tab_parts
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 4096, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024;

INSERT INTO tab_parts SELECT * FROM tab WHERE id < 70000;
INSERT INTO tab_parts SELECT * FROM tab WHERE id >= 70000 AND id < 140000;
INSERT INTO tab_parts SELECT * FROM tab WHERE id >= 140000;

SELECT count(), sum(cityHash64(id)) FROM tab_parts WHERE hasAllTokens(s, ['a', 'b', 'c']) SETTINGS max_threads = 3;

CREATE TABLE tab_wide
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 4096, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_wide SELECT * FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_wide WHERE hasAllTokens(s, ['a', 'b', 'c']);
SELECT count(), sum(cityHash64(id)) FROM tab_wide WHERE hasToken(s, 'r');

CREATE TABLE tab_mixed
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 4096, posting_list_codec = 'none')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_mixed SELECT * FROM tab;

SELECT count(), sum(cityHash64(id)) FROM tab_mixed WHERE hasAllTokens(s, ['a', 'b', 'c']);
SELECT count(), sum(cityHash64(id)) FROM tab_mixed WHERE hasAllTokens(s, ['r', 'b']);

DROP TABLE tab;
DROP TABLE tab_parts;
DROP TABLE tab_wide;
DROP TABLE tab_mixed;
