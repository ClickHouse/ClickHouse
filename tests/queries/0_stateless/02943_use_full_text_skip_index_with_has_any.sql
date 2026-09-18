DROP TABLE IF EXISTS tokenbf_v1_hasany_test;
DROP TABLE IF EXISTS ngrambf_v1_hasany_test;

CREATE TABLE tokenbf_v1_hasany_test
(
    id UInt32,
    array Array(String),
    INDEX idx_array_tokenbf_v1 array TYPE tokenbf_v1(512,3,0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE ngrambf_v1_hasany_test
(
    id UInt32,
    array Array(String),
    INDEX idx_array_ngrambf_v1 array TYPE ngrambf_v1(3,512,3,0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tokenbf_v1_hasany_test VALUES (1, ['this is a test', 'example.com']), (2, ['another test', 'another example']);
INSERT INTO ngrambf_v1_hasany_test VALUES (1, ['this is a test', 'example.com']), (2, ['another test', 'another example']);

SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['this is a test']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['example.com']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['another test']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['another example', 'example.com']) ORDER BY id ASC SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';

SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['this is a test']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['example.com']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['another test']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['another example', 'example.com']) ORDER BY id ASC SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';

SELECT * FROM tokenbf_v1_hasany_test WHERE hasAll(array, ['this is a test', 'example.com']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAll(array, ['another test']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAll(array, ['another example', 'example.com']) ORDER BY id ASC SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';

SELECT * FROM ngrambf_v1_hasany_test WHERE hasAll(array, ['this is a test', 'example.com']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAll(array, ['another test']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAll(array, ['another example', 'example.com']) ORDER BY id ASC SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT count() FROM tokenbf_v1_hasany_test WHERE hasAll(array, []);

DROP TABLE tokenbf_v1_hasany_test;
DROP TABLE ngrambf_v1_hasany_test;

SELECT 'hasAnyTokens and hasAllTokens';

SET explain_query_plan_default = 'legacy';
SET parallel_replicas_local_plan = 1; -- this setting may skip index analysis when false
SET use_skip_indexes_on_data_read = 0; -- the granule counts below are the ones index analysis decided
SET use_query_condition_cache = 0; -- every case runs the same predicate more than once

DROP TABLE IF EXISTS token_search_tokenbf;
DROP TABLE IF EXISTS token_search_ngrambf;
DROP TABLE IF EXISTS token_search_sparsegrams;
DROP TABLE IF EXISTS token_search_text_tokenizer;
DROP TABLE IF EXISTS token_search_text_preprocessor;
DROP TABLE IF EXISTS token_search_text_other_column;
DROP TABLE IF EXISTS token_search_map;

CREATE TABLE token_search_tokenbf
(
    id UInt32,
    msg String,
    INDEX idx_msg_tokenbf msg TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE token_search_ngrambf
(
    id UInt32,
    msg String,
    INDEX idx_msg_ngrambf msg TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE token_search_sparsegrams
(
    id UInt32,
    msg String,
    INDEX idx_msg_sparsegrams msg TYPE sparse_grams(3, 100, 512, 3, 0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO token_search_tokenbf VALUES (1, 'alpha beta'), (2, 'gamma delta'), (3, 'alpha epsilon'), (4, 'zeta eta'), (5, 'ab cd');
INSERT INTO token_search_ngrambf VALUES (1, 'alpha beta'), (2, 'gamma delta'), (3, 'alpha epsilon'), (4, 'zeta eta'), (5, 'ab cd');
INSERT INTO token_search_sparsegrams VALUES (1, 'alpha beta'), (2, 'gamma delta'), (3, 'alpha epsilon'), (4, 'zeta eta'), (5, 'ab cd');

SELECT 'tokenbf_v1, granules pruned';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, ['gamma'])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, ['alpha', 'beta'])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, ['nosuchtoken'])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';

SELECT 'tokenbf_v1, answers unchanged by the index';
SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, ['alpha']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf';
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, ['alpha']) ORDER BY id SETTINGS ignore_data_skipping_indices = 'idx_msg_tokenbf';
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, ['alpha', 'epsilon']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf';
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, ['alpha', 'epsilon']) ORDER BY id SETTINGS ignore_data_skipping_indices = 'idx_msg_tokenbf';

SELECT 'A String needle stays unindexed';
SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, 'alpha') ORDER BY id;
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, 'alpha beta') ORDER BY id;
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, 'alpha') SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf'; -- { serverError INDEX_NOT_USED }
SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, 'alpha beta') SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf'; -- { serverError INDEX_NOT_USED }

SELECT 'An explicit tokenizer argument stays unindexed';
-- `alp` is a verbatim token of the tokenizer named here, not of the one `idx_msg_tokenbf` holds, so
-- routing this shape would prune granules that match.
SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, ['alp'], 'ngrams(3)') ORDER BY id;
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, ['alp', 'pha'], 'ngrams(3)') ORDER BY id;
SELECT '--';
SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, ['alp'], 'ngrams(3)') SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf'; -- { serverError INDEX_NOT_USED }
SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, ['alp', 'pha'], 'ngrams(3)') SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf'; -- { serverError INDEX_NOT_USED }

SELECT 'An empty needle array';
-- No needle filter: the hasAnyTokens fold drops every granule, the hasAllTokens one keeps them all.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_tokenbf WHERE hasAnyTokens(msg, [])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_tokenbf WHERE hasAllTokens(msg, [])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT count() FROM token_search_tokenbf WHERE hasAnyTokens(msg, []) SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf';
SELECT count() FROM token_search_tokenbf WHERE hasAnyTokens(msg, []) SETTINGS ignore_data_skipping_indices = 'idx_msg_tokenbf';
SELECT count() FROM token_search_tokenbf WHERE hasAllTokens(msg, []) SETTINGS force_data_skipping_indices = 'idx_msg_tokenbf';
SELECT count() FROM token_search_tokenbf WHERE hasAllTokens(msg, []) SETTINGS ignore_data_skipping_indices = 'idx_msg_tokenbf';

SELECT 'ngrambf_v1';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_ngrambf WHERE hasAnyTokens(msg, ['gamma'])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT id FROM token_search_ngrambf WHERE hasAnyTokens(msg, ['gamma']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_msg_ngrambf';
SELECT '--';
-- A needle shorter than the ngram size builds an empty filter, which every granule contains.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_ngrambf WHERE hasAnyTokens(msg, ['ab'])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT id FROM token_search_ngrambf WHERE hasAnyTokens(msg, ['ab']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_msg_ngrambf';
SELECT '--';
SELECT id FROM token_search_ngrambf WHERE hasAnyTokens(msg, ['ab']) ORDER BY id SETTINGS ignore_data_skipping_indices = 'idx_msg_ngrambf';

SELECT 'sparse_grams';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id FROM token_search_sparsegrams WHERE hasAnyTokens(msg, ['gamma'])
) WHERE explain LIKE 'Description%' OR explain LIKE 'Granules%';
SELECT '--';
SELECT id FROM token_search_sparsegrams WHERE hasAnyTokens(msg, ['gamma']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_msg_sparsegrams';
SELECT '--';
SELECT id FROM token_search_sparsegrams WHERE hasAnyTokens(msg, ['gamma']) ORDER BY id SETTINGS ignore_data_skipping_indices = 'idx_msg_sparsegrams';

CREATE TABLE token_search_text_tokenizer
(
    id UInt32,
    msg String,
    INDEX idx_txt msg TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX idx_bf msg TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO token_search_text_tokenizer VALUES (1, 'alpha beta');

CREATE TABLE token_search_text_preprocessor
(
    id UInt32,
    msg String,
    INDEX idx_txt msg TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg)) GRANULARITY 1,
    INDEX idx_bf msg TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO token_search_text_preprocessor VALUES (1, 'MixedCase Token');

SELECT 'A text index on the same column, tokenizer mismatch';
SELECT id FROM token_search_text_tokenizer WHERE hasAnyTokens(msg, ['alp']);
SELECT '--';
SELECT id FROM token_search_text_tokenizer WHERE hasAnyTokens(msg, ['alp']) SETTINGS ignore_data_skipping_indices = 'idx_bf';
SELECT '--';
SELECT id FROM token_search_text_tokenizer WHERE hasAnyTokens(msg, ['alp']) SETTINGS force_data_skipping_indices = 'idx_bf'; -- { serverError INDEX_NOT_USED }

SELECT 'A text index on the same column, preprocessor mismatch';
SELECT id FROM token_search_text_preprocessor WHERE hasAnyTokens(msg, ['mixedcase']);
SELECT '--';
SELECT id FROM token_search_text_preprocessor WHERE hasAnyTokens(msg, ['mixedcase']) SETTINGS ignore_data_skipping_indices = 'idx_bf';
SELECT '--';
SELECT id FROM token_search_text_preprocessor WHERE hasAnyTokens(msg, ['mixedcase']) SETTINGS force_data_skipping_indices = 'idx_bf'; -- { serverError INDEX_NOT_USED }

CREATE TABLE token_search_text_other_column
(
    id UInt32,
    msg String,
    other String,
    INDEX idx_bf msg TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX idx_txt other TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO token_search_text_other_column VALUES (1, 'alpha beta', 'x'), (2, 'gamma delta', 'y');

CREATE TABLE token_search_map
(
    id UInt32,
    m Map(String, String),
    INDEX idx_bf mapValues(m) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX idx_txt m['k'] TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO token_search_map VALUES (1, {'k':'alpha beta'});

-- A text index anywhere in the table decides this, because an index description carries the name of
-- the expression it indexes: a text index on `m['k']` and this index's `mapValues(m)` are different
-- names for data a map element predicate reaches through either.
SELECT 'A text index on another column';
SELECT id FROM token_search_text_other_column WHERE hasAnyTokens(msg, ['alpha']);
SELECT '--';
SELECT id FROM token_search_text_other_column WHERE hasAnyTokens(msg, ['alpha']) SETTINGS ignore_data_skipping_indices = 'idx_bf';
SELECT '--';
SELECT id FROM token_search_text_other_column WHERE hasAnyTokens(msg, ['alpha']) SETTINGS force_data_skipping_indices = 'idx_bf'; -- { serverError INDEX_NOT_USED }

SELECT 'A text index on a map element';
-- optimize_functions_to_subcolumns decides which tokenizer a map element predicate resolves, and with
-- it the answer below; that holds with and without this index and is not what this case is about.
SELECT id FROM token_search_map WHERE hasAnyTokens(m['k'], ['alpha']) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT '--';
SELECT id FROM token_search_map WHERE hasAnyTokens(m['k'], ['alpha']) SETTINGS force_data_skipping_indices = 'idx_bf'; -- { serverError INDEX_NOT_USED }

DROP TABLE token_search_tokenbf;
DROP TABLE token_search_ngrambf;
DROP TABLE token_search_sparsegrams;
DROP TABLE token_search_text_tokenizer;
DROP TABLE token_search_text_preprocessor;
DROP TABLE token_search_text_other_column;
DROP TABLE token_search_map;
