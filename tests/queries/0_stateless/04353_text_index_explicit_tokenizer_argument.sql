SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'goodbye world'), (3, 'unrelated text'), (4, 'more unrelated');

SELECT '-- matching tokenizer uses the index';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE hasAllTokens(doc, ['hello', 'world'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- matching tokenizer prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha')
    SETTINGS query_plan_direct_read_from_text_index = 0
) WHERE explain ILIKE '%Granules: 1/2%';

SELECT '-- matching tokenizer reaches the direct read';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha')
    SETTINGS query_plan_direct_read_from_text_index = 1
) WHERE explain ILIKE '%__text_index_idx_hasAnyTokens%';

SELECT '-- registered alias of the same tokenizer uses the index';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'tokenbf_v1') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- whitespace variants of the definition use the index';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], ' splitByNonAlpha ') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- a different tokenizer still full-scans';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'ngrams(3)') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'array') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT '-- 2-argument form is unchanged';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- results agree across use_skip_indexes x query_plan_direct_read_from_text_index';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM tab WHERE hasAllTokens(doc, ['world'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab WHERE hasAllTokens(doc, ['world'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT id FROM tab WHERE hasAllTokens(doc, ['world'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- the same predicate in PREWHERE and WHERE';
SELECT id FROM tab PREWHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id;

SELECT '-- hasPhrase';
SELECT id FROM tab WHERE hasPhrase(doc, 'hello world', 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE hasPhrase(doc, 'hello world', 'ngrams(3)') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT '-- an invalid or non-constant tokenizer argument is still rejected';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'nonExistentTokenizer'); -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], materialize('splitByNonAlpha')); -- { serverError ILLEGAL_COLUMN }

DROP TABLE tab;

SELECT '-- tokenizer parameters are not normalized away';

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'goodbye world'), (3, 'unrelated text'), (4, 'more unrelated');

SELECT id FROM tab WHERE hasAnyTokens(doc, ['hel'], 'ngrams(3)') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hell'], 'ngrams(4)') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;

SELECT '-- an index with a postprocessor still full-scans';

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(doc = 'the', '', doc)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'see the cat'), (2, 'see a cat'), (3, 'see cat'), (4, 'the cat see'), (5, 'cat see');

SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab WHERE hasAllTokens(doc, ['the', 'cat'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasAllTokens(doc, ['the', 'cat'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- an index with a preprocessor still full-scans';

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(doc)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'Hello World'), (2, 'hello there'), (3, 'goodbye');

SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab WHERE hasAllTokens(doc, ['hello', 'world'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasAllTokens(doc, ['hello', 'world'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab WHERE hasPhrase(doc, 'hello world', 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasPhrase(doc, 'hello world', 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- expression index';

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx lower(doc) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'Hello World'), (2, 'Goodbye World'), (3, 'unrelated text'), (4, 'more unrelated');

SELECT id FROM tab WHERE hasAnyTokens(lower(doc), ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE hasAnyTokens(lower(doc), ['hello'], 'ngrams(3)') ORDER BY id SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;
