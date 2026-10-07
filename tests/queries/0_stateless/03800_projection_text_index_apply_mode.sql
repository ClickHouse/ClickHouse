SET allow_experimental_projection_text_index = 1;
-- Test that has_block_index=0 and has_block_index=1 produce identical results.
-- has_block_index controls whether .pidx skip data is written. When disabled,
-- PostingListCursor falls back to sequential scan of .pst packed blocks.
-- Both modes must return the same query results.

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

----------------------------------------------------
SELECT 'Test basic queries: has_block_index=1 vs has_block_index=0';

DROP TABLE IF EXISTS tab_idx;
DROP TABLE IF EXISTS tab_noidx;

CREATE TABLE tab_idx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 1))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

CREATE TABLE tab_noidx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 0))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_idx VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick a10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');
INSERT INTO tab_noidx VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick a10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');

-- hasToken
SELECT * FROM tab_idx WHERE hasToken(s, 'Alick') ORDER BY k;
SELECT * FROM tab_noidx WHERE hasToken(s, 'Alick') ORDER BY k;

-- equals
SELECT * FROM tab_idx WHERE s == 'Alick a01';
SELECT * FROM tab_noidx WHERE s == 'Alick a01';

-- hasAnyTokens
SELECT * FROM tab_idx WHERE hasAnyTokens(s, ['Alick', 'Click']) ORDER BY k;
SELECT * FROM tab_noidx WHERE hasAnyTokens(s, ['Alick', 'Click']) ORDER BY k;

-- hasAllTokens
SELECT * FROM tab_idx WHERE hasAllTokens(s, ['Alick', 'a01']) ORDER BY k;
SELECT * FROM tab_noidx WHERE hasAllTokens(s, ['Alick', 'a01']) ORDER BY k;

-- IN
SELECT * FROM tab_idx WHERE s IN ('Alick a01', 'Alick a06') ORDER BY k;
SELECT * FROM tab_noidx WHERE s IN ('Alick a01', 'Alick a06') ORDER BY k;

-- empty result
SELECT * FROM tab_idx WHERE hasToken(s, 'nonexistent') ORDER BY k;
SELECT * FROM tab_noidx WHERE hasToken(s, 'nonexistent') ORDER BY k;

----------------------------------------------------
SELECT 'Test after merge: has_block_index=1 vs has_block_index=0';

DROP TABLE IF EXISTS tab_merge_idx;
DROP TABLE IF EXISTS tab_merge_noidx;

CREATE TABLE tab_merge_idx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = ngrams(2), has_block_index = 1))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

CREATE TABLE tab_merge_noidx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = ngrams(2), has_block_index = 0))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_merge_idx VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04');
INSERT INTO tab_merge_idx VALUES (201, 'rick c01'), (202, 'mick c02'), (203, 'nick c03');
INSERT INTO tab_merge_noidx VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04');
INSERT INTO tab_merge_noidx VALUES (201, 'rick c01'), (202, 'mick c02'), (203, 'nick c03');

OPTIMIZE TABLE tab_merge_idx FINAL;
OPTIMIZE TABLE tab_merge_noidx FINAL;

SELECT * FROM tab_merge_idx WHERE s LIKE '%01%' ORDER BY k;
SELECT * FROM tab_merge_noidx WHERE s LIKE '%01%' ORDER BY k;

----------------------------------------------------
SELECT 'Test large dataset with block-level index boundary';

DROP TABLE IF EXISTS tab_large_idx;
DROP TABLE IF EXISTS tab_large_noidx;

CREATE TABLE tab_large_idx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 1, posting_list_block_size = 256))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE tab_large_noidx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 0))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_large_idx SELECT number, concat('token', toString(number % 10), ' word', toString(number)) FROM numbers(500);
INSERT INTO tab_large_noidx SELECT number, concat('token', toString(number % 10), ' word', toString(number)) FROM numbers(500);

SELECT count() FROM tab_large_idx WHERE hasToken(s, 'token0');
SELECT count() FROM tab_large_noidx WHERE hasToken(s, 'token0');

SELECT count() FROM tab_large_idx WHERE hasToken(s, 'token5');
SELECT count() FROM tab_large_noidx WHERE hasToken(s, 'token5');

SELECT count() FROM tab_large_idx WHERE hasAllTokens(s, ['token3', 'word3']);
SELECT count() FROM tab_large_noidx WHERE hasAllTokens(s, ['token3', 'word3']);

SELECT count() FROM tab_large_idx WHERE hasAnyTokens(s, ['word0', 'word1', 'word2']);
SELECT count() FROM tab_large_noidx WHERE hasAnyTokens(s, ['word0', 'word1', 'word2']);

----------------------------------------------------
SELECT 'Test large dataset after merge';

DROP TABLE IF EXISTS tab_lm_idx;
DROP TABLE IF EXISTS tab_lm_noidx;

CREATE TABLE tab_lm_idx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 1, posting_list_block_size = 256))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE tab_lm_noidx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 0))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_lm_idx SELECT number, concat('token', toString(number % 10), ' word', toString(number)) FROM numbers(250);
INSERT INTO tab_lm_idx SELECT number + 250, concat('token', toString((number + 250) % 10), ' word', toString(number + 250)) FROM numbers(250);
INSERT INTO tab_lm_noidx SELECT number, concat('token', toString(number % 10), ' word', toString(number)) FROM numbers(250);
INSERT INTO tab_lm_noidx SELECT number + 250, concat('token', toString((number + 250) % 10), ' word', toString(number + 250)) FROM numbers(250);

OPTIMIZE TABLE tab_lm_idx FINAL;
OPTIMIZE TABLE tab_lm_noidx FINAL;

SELECT count() FROM tab_lm_idx WHERE hasToken(s, 'token0');
SELECT count() FROM tab_lm_noidx WHERE hasToken(s, 'token0');

SELECT count() FROM tab_lm_idx WHERE hasToken(s, 'token5');
SELECT count() FROM tab_lm_noidx WHERE hasToken(s, 'token5');

----------------------------------------------------
SELECT 'Test UTF-8 with ngram tokenizer';

DROP TABLE IF EXISTS tab_utf8_idx;
DROP TABLE IF EXISTS tab_utf8_noidx;

CREATE TABLE tab_utf8_idx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = ngrams(2), has_block_index = 1))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

CREATE TABLE tab_utf8_noidx(k UInt64, s String,
    PROJECTION af INDEX s TYPE text(tokenizer = ngrams(2), has_block_index = 0))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_utf8_idx VALUES (1, '你好世界'), (2, '世界和平'), (3, '你好朋友'), (4, '天气很好');
INSERT INTO tab_utf8_noidx VALUES (1, '你好世界'), (2, '世界和平'), (3, '你好朋友'), (4, '天气很好');

SELECT * FROM tab_utf8_idx WHERE s LIKE '%你好%' ORDER BY k;
SELECT * FROM tab_utf8_noidx WHERE s LIKE '%你好%' ORDER BY k;

SELECT * FROM tab_utf8_idx WHERE s LIKE '%世界%' ORDER BY k;
SELECT * FROM tab_utf8_noidx WHERE s LIKE '%世界%' ORDER BY k;

----------------------------------------------------
DROP TABLE IF EXISTS tab_idx;
DROP TABLE IF EXISTS tab_noidx;
DROP TABLE IF EXISTS tab_merge_idx;
DROP TABLE IF EXISTS tab_merge_noidx;
DROP TABLE IF EXISTS tab_large_idx;
DROP TABLE IF EXISTS tab_large_noidx;
DROP TABLE IF EXISTS tab_lm_idx;
DROP TABLE IF EXISTS tab_lm_noidx;
DROP TABLE IF EXISTS tab_utf8_idx;
DROP TABLE IF EXISTS tab_utf8_noidx;
