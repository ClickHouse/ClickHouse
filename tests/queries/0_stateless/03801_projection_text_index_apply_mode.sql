-- Tags: no-fasttest
-- no-fasttest: It can be slow

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test text_index_posting_list_apply_mode = materialize (default)';

DROP TABLE IF EXISTS tab_mode;

CREATE TABLE tab_mode(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_mode VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04'), (105, 'Elick a05'), (106, 'Alick a06'), (107, 'Blick a07'), (108, 'Click a08'), (109, 'Dlick a09'), (110, 'Elick a10'), (111, 'Alick b01'), (112, 'Blick b02'), (113, 'Click b03'), (114, 'Dlick b04'), (115, 'Elick b05'), (116, 'Alick b06'), (117, 'Blick b07'), (118, 'Click b08'), (119, 'Dlick b09'), (120, 'Elick b10');

-- materialize mode: search with hasToken
SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') ORDER BY k;

-- materialize mode: search with ==
SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE s == 'Alick a01';

-- materialize mode: search with LIKE
SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE s LIKE '%01%' ORDER BY k;

----------------------------------------------------
SELECT 'Test text_index_posting_list_apply_mode = lazy';

-- lazy mode: search with hasToken
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') ORDER BY k;

-- lazy mode: search with ==
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE s == 'Alick a01';

-- lazy mode: search with LIKE
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE s LIKE '%01%' ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with multi-token AND (hasToken intersection)';

-- lazy mode: multi-token AND search
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') AND hasToken(s, 'a01') ORDER BY k;

-- materialize mode: same query for comparison
SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') AND hasToken(s, 'a01') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with IN operator';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE s IN ('Alick a01', 'Alick a06') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE s IN ('Alick a01', 'Alick a06') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with merged parts';

DROP TABLE IF EXISTS tab_merge;

CREATE TABLE tab_merge(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = ngrams(2)))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_merge VALUES (101, 'Alick a01'), (102, 'Blick a02'), (103, 'Click a03'), (104, 'Dlick a04');
INSERT INTO tab_merge VALUES (201, 'rick c01'), (202, 'mick c02'), (203, 'nick c03');

OPTIMIZE TABLE tab_merge FINAL;

-- lazy mode after merge
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_merge WHERE s LIKE '%01%' ORDER BY k;

-- materialize mode after merge
SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_merge WHERE s LIKE '%01%' ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with hasAnyTokens (OR union)';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasAnyTokens(s, ['Alick', 'Click']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasAnyTokens(s, ['Alick', 'Click']) ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with hasAllTokens (AND intersection)';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasAllTokens(s, ['Alick', 'a01']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasAllTokens(s, ['Alick', 'a01']) ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with hasAnyTokens string form';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasAnyTokens(s, 'Blick Click') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasAnyTokens(s, 'Blick Click') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with hasAllTokens string form';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasAllTokens(s, 'Alick a06') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasAllTokens(s, 'Alick a06') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with empty result';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasToken(s, 'nonexistent') ORDER BY k;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasAllTokens(s, ['Alick', 'nonexistent']) ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with large dataset (>128 rows, TurboPFor block boundary)';

DROP TABLE IF EXISTS tab_large;

CREATE TABLE tab_large(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_large SELECT number, concat('token_', toString(number % 10), ' word_', toString(number)) FROM numbers(500);

SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_large WHERE hasToken(s, 'token_0');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_large WHERE hasToken(s, 'token_0');

SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_large WHERE hasToken(s, 'token_5');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_large WHERE hasToken(s, 'token_5');

-- lazy mode: AND intersection on large dataset
SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_large WHERE hasAllTokens(s, ['token_3', 'word_3']);

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_large WHERE hasAllTokens(s, ['token_3', 'word_3']);

-- lazy mode: OR union on large dataset
SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_large WHERE hasAnyTokens(s, ['word_0', 'word_1', 'word_2']);

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_large WHERE hasAnyTokens(s, ['word_0', 'word_1', 'word_2']);

----------------------------------------------------
SELECT 'Test lazy mode with multiple segments (multi-insert without merge)';

DROP TABLE IF EXISTS tab_multi;

CREATE TABLE tab_multi(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_multi VALUES (1, 'alpha beta'), (2, 'gamma delta'), (3, 'alpha gamma'), (4, 'beta delta');
INSERT INTO tab_multi VALUES (5, 'alpha epsilon'), (6, 'beta zeta'), (7, 'gamma eta'), (8, 'delta theta');
INSERT INTO tab_multi VALUES (9, 'alpha iota'), (10, 'beta kappa');

-- lazy mode: query across multiple parts
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_multi WHERE hasToken(s, 'alpha') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_multi WHERE hasToken(s, 'alpha') ORDER BY k;

-- lazy mode: AND across parts
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_multi WHERE hasAllTokens(s, ['alpha', 'beta']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_multi WHERE hasAllTokens(s, ['alpha', 'beta']) ORDER BY k;

-- lazy mode: OR across parts
SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_multi WHERE hasAnyTokens(s, ['epsilon', 'zeta']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_multi WHERE hasAnyTokens(s, ['epsilon', 'zeta']) ORDER BY k;

-- Now merge and test again
OPTIMIZE TABLE tab_multi FINAL;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_multi WHERE hasToken(s, 'alpha') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_multi WHERE hasToken(s, 'alpha') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with UTF-8 data and ngram tokenizer';

DROP TABLE IF EXISTS tab_utf8;

CREATE TABLE tab_utf8(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = ngrams(2)))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_utf8 VALUES (1, '你好世界'), (2, '世界和平'), (3, '你好朋友'), (4, '天气很好');

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_utf8 WHERE s LIKE '%你好%' ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_utf8 WHERE s LIKE '%你好%' ORDER BY k;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_utf8 WHERE s LIKE '%世界%' ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_utf8 WHERE s LIKE '%世界%' ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with 3-way AND intersection';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') AND hasToken(s, 'a01') ORDER BY k;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') AND hasToken(s, 'b06') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') AND hasToken(s, 'b06') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode with OR of hasToken';

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') OR hasToken(s, 'Blick') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_mode WHERE hasToken(s, 'Alick') OR hasToken(s, 'Blick') ORDER BY k;

----------------------------------------------------
SELECT 'Test lazy mode consistency with direct read';

DROP TABLE IF EXISTS tab_dr;

CREATE TABLE tab_dr(k UInt64, text String, PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab_dr VALUES (1, 'hello world'), (2, 'foo bar'), (3, 'hello foo'), (4, 'bar baz');

SET query_plan_direct_read_from_text_index = 1;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_dr WHERE hasToken(text, 'hello') ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_dr WHERE hasToken(text, 'hello') ORDER BY k;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_dr WHERE hasAllTokens(text, ['hello', 'world']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_dr WHERE hasAllTokens(text, ['hello', 'world']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT * FROM tab_dr WHERE hasAnyTokens(text, ['hello', 'bar']) ORDER BY k;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT * FROM tab_dr WHERE hasAnyTokens(text, ['hello', 'bar']) ORDER BY k;

SET query_plan_direct_read_from_text_index = 0;

----------------------------------------------------
SELECT 'Test lazy mode with large posting list and block-level index';

DROP TABLE IF EXISTS tab_block_idx;

-- Use small posting_list_block_size (256) to trigger multiple large blocks with fewer rows.
-- This exercises the v2 block-level index layout where each large block has a trailing index section.
CREATE TABLE tab_block_idx(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 256))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_block_idx SELECT number, concat('token_', toString(number % 10), ' word_', toString(number)) FROM numbers(500);

SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_block_idx WHERE hasToken(s, 'token_0');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_block_idx WHERE hasToken(s, 'token_0');

SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_block_idx WHERE hasToken(s, 'token_5');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_block_idx WHERE hasToken(s, 'token_5');

-- AND intersection with block-level index
SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_block_idx WHERE hasAllTokens(s, ['token_3', 'word_3']);

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_block_idx WHERE hasAllTokens(s, ['token_3', 'word_3']);

-- OR union with block-level index
SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_block_idx WHERE hasAnyTokens(s, ['word_0', 'word_1', 'word_2']);

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_block_idx WHERE hasAnyTokens(s, ['word_0', 'word_1', 'word_2']);

----------------------------------------------------
SELECT 'Test lazy mode with large posting list block-level index after merge';

DROP TABLE IF EXISTS tab_block_merge;

CREATE TABLE tab_block_merge(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 256))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_block_merge SELECT number, concat('token_', toString(number % 10), ' word_', toString(number)) FROM numbers(250);
INSERT INTO tab_block_merge SELECT number + 250, concat('token_', toString((number + 250) % 10), ' word_', toString(number + 250)) FROM numbers(250);

OPTIMIZE TABLE tab_block_merge FINAL;

SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_block_merge WHERE hasToken(s, 'token_0');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_block_merge WHERE hasToken(s, 'token_0');

SET text_index_posting_list_apply_mode = 'lazy';
SELECT count() FROM tab_block_merge WHERE hasToken(s, 'token_5');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_block_merge WHERE hasToken(s, 'token_5');

----------------------------------------------------
DROP TABLE IF EXISTS tab_mode;
DROP TABLE IF EXISTS tab_merge;
DROP TABLE IF EXISTS tab_large;
DROP TABLE IF EXISTS tab_multi;
DROP TABLE IF EXISTS tab_utf8;
DROP TABLE IF EXISTS tab_dr;
DROP TABLE IF EXISTS tab_block_idx;
DROP TABLE IF EXISTS tab_block_merge;
