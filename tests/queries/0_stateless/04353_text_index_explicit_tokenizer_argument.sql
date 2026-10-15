-- Tests that hasAnyTokens, hasAllTokens and hasPhrase use a text index when their third argument
-- names the tokenizer of that index, and fall back to a full scan when it names a different one.

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

SELECT '-- a postprocessor is applied, like in the two-argument form';

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

-- A term the postprocessor keeps is still searchable, so the empty results below are a property of
-- the postprocessor and not of a table that matches nothing.
SELECT id FROM tab WHERE hasAnyTokens(doc, ['a'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

-- The postprocessor maps 'the' to the empty string, so the term is absent from the index and the
-- needle reduces to no token: neither form matches, whether or not the index is read.
SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

-- The two forms agree for the same setting, for each function and each value of use_skip_indexes.
-- A row prints only if they disagree, so the expected output is empty.
SELECT 'hasAnyTokens', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['the']) ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;
SELECT 'hasAnyTokens', 0, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['the']) ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 0;
SELECT 'hasAllTokens', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(doc, ['the', 'cat'], 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(doc, ['the', 'cat']) ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;

-- The rows themselves, so a change of behaviour is visible and not only a parity assertion.
SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['the'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- a preprocessor is applied, like in the two-argument form';

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

SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

-- Same parity assertion as for the postprocessor above, for all three functions. Expected output is empty.
SELECT 'hasAnyTokens', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello']) ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;
SELECT 'hasAnyTokens', 0, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello']) ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 0;
SELECT 'hasAllTokens', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(doc, ['hello', 'world'], 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(doc, ['hello', 'world']) ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;
SELECT 'hasPhrase', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'hello world', 'splitByNonAlpha') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'hello world') ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;

SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['hello'], 'splitByNonAlpha') ORDER BY id SETTINGS use_skip_indexes = 0;

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

SELECT '-- postprocessor with an explicit separator list';

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByString(['()']), postprocessor = lower(doc)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- The data is already lowercase, so the postprocessor does not change it.
INSERT INTO tab VALUES (1, 'a()bc()d'), (2, 'zz');

-- hasPhrase searches the postprocessed tokens rejoined with a space, and splitByString(['()'])
-- keeps that space inside a token, so a phrase of several tokens never matches here. Both forms
-- share that rewrite, so they agree; a row prints only if they disagree.
SELECT 'hasPhrase splitByString', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'bc()d', 'splitByString([''()''])') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'bc()d') ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;
SELECT 'hasPhrase splitByString', 0, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'bc()d', 'splitByString([''()''])') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'bc()d') ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 0;

-- The index answers the phrase, and hasAnyTokens and hasAllTokens below match the postprocessed
-- tokens verbatim, so the empty phrase result is a property of the rejoin and not of a dead index.
SELECT id FROM tab WHERE hasPhrase(doc, 'bc()d', 'splitByString([''()''])') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE hasAnyTokens(doc, 'bc', 'splitByString([''()''])') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT id FROM tab WHERE hasAllTokens(doc, 'bc()d', 'splitByString([''()''])') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByString([' ', 'x']), postprocessor = lower(doc)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- The postprocessor turns the stored token 'X' into 'x', which is itself a separator here, so
-- rejoining and re-splitting drops it and makes the non-consecutive 'a b' look consecutive.
INSERT INTO tab VALUES (1, 'A X B'), (2, 'zz');

SELECT 'hasPhrase separator from postprocessor', 1, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'a b', 'splitByString(['' '', ''x''])') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'a b') ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 1;
SELECT 'hasPhrase separator from postprocessor', 0, three, two FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'a b', 'splitByString(['' '', ''x''])') ORDER BY id)) AS three,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'a b') ORDER BY id)) AS two
) WHERE three != two SETTINGS use_skip_indexes = 0;

SELECT id FROM tab WHERE hasPhrase(doc, 'a b', 'splitByString(['' '', ''x''])') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = ngrams(3), postprocessor = lower(doc)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'abcdef'), (2, 'zzzzzz');

-- A phrase stays a substring of the joined grams, so ngrams still uses the index.
SELECT id FROM tab WHERE hasPhrase(doc, 'bcd', 'ngrams(3)') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'hasPhrase ngrams', indexed, scanned FROM (
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'bcd', 'ngrams(3)') ORDER BY id SETTINGS use_skip_indexes = 1)) AS indexed,
           (SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(doc, 'bcd', 'ngrams(3)') ORDER BY id SETTINGS use_skip_indexes = 0)) AS scanned
) WHERE indexed != scanned;

-- A gram that spans a separator is not representable as a postprocessed token, and both forms must
-- agree on rejecting it, under either direct-read mode, rather than one of them answering from the
-- index. The phrase does not occur literally, so the rejection is not masking a match.
SELECT id FROM tab WHERE hasPhrase(doc, 'cd e', 'ngrams(3)') SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE hasPhrase(doc, 'cd e', 'ngrams(3)') SETTINGS query_plan_direct_read_from_text_index = 1; -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE hasPhrase(doc, 'cd e') SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE hasPhrase(doc, 'cd e') SETTINGS query_plan_direct_read_from_text_index = 1; -- { serverError BAD_ARGUMENTS }
SELECT id FROM tab WHERE position(doc, 'cd e') > 0 ORDER BY id;

DROP TABLE tab;
