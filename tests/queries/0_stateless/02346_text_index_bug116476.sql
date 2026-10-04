-- Tags: no-parallel-replicas

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/116476.
-- A stop-word postprocessor spelled with `IN` must drop the same tokens as one spelled with `=`.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_eq;
DROP TABLE IF EXISTS tab_in;
DROP TABLE IF EXISTS tab_not_in;
DROP TABLE IF EXISTS tab_array;
DROP TABLE IF EXISTS tab_multi_if;
DROP TABLE IF EXISTS tab_preprocessor;

CREATE TABLE tab_eq
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message = 'the', '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

CREATE TABLE tab_in
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message IN ('the'), '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

CREATE TABLE tab_not_in
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message NOT IN ('see', 'cat'), '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

CREATE TABLE tab_array
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message IN ['the', 'a'], '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

CREATE TABLE tab_multi_if
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = multiIf(message IN ('the'), '', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_eq VALUES (1, 'see the cat'), (2, 'the see cat'), (3, 'see cat'), (4, 'cat see'), (5, 'see a cat');
INSERT INTO tab_in VALUES (1, 'see the cat'), (2, 'the see cat'), (3, 'see cat'), (4, 'cat see'), (5, 'see a cat');
INSERT INTO tab_not_in VALUES (1, 'see the cat'), (2, 'the see cat'), (3, 'see cat'), (4, 'cat see'), (5, 'see a cat');
INSERT INTO tab_array VALUES (1, 'see the cat'), (2, 'the see cat'), (3, 'see cat'), (4, 'cat see'), (5, 'see a cat');
INSERT INTO tab_multi_if VALUES (1, 'see the cat'), (2, 'the see cat'), (3, 'see cat'), (4, 'cat see'), (5, 'see a cat');

SELECT '1. The IN spelling drops the same tokens as the = spelling, so it indexes the same tokens.';

SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_eq, idx);
SELECT arrayStringConcat(arraySort(groupArray(token)), ' ') FROM mergeTreeTextIndex(currentDatabase(), tab_in, idx);

SELECT '2. Stop words spelled with IN leave no positional gap: [1,2,3].';

-- Force the row-level fallback of the direct read; the sections below use the default selectivity.
SELECT arraySort(groupArray(id)) FROM tab_eq WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 0;
SELECT arraySort(groupArray(id)) FROM tab_in WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 0;
-- Without direct read from the index.
SELECT arraySort(groupArray(id)) FROM tab_eq WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM tab_in WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '3. The needle is postprocessed too, so a needle with a stop word matches as well: [1,2,3].';

SELECT arraySort(groupArray(id)) FROM tab_in WHERE hasPhrase(message, 'see the cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_in WHERE hasPhrase(message, 'see the cat') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '4. Reversed order still does not match: [4].';

SELECT arraySort(groupArray(id)) FROM tab_in WHERE hasPhrase(message, 'cat see') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_in WHERE hasPhrase(message, 'cat see') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '5. NOT IN keeps only the listed tokens, every other token is dropped: [1,2,3,5].';

SELECT arraySort(groupArray(id)) FROM tab_not_in WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_not_in WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '6. An array right-hand side drops both stop words: [1,2,3,5].';

SELECT arraySort(groupArray(id)) FROM tab_array WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_array WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '7. The multiIf spelling behaves like if: [1,2,3].';

SELECT arraySort(groupArray(id)) FROM tab_multi_if WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_multi_if WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '8. An IN filter in the preprocessor is reconstructed correctly too: [2,3,4].';

CREATE TABLE tab_preprocessor
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = if(message IN ('cat see'), 'see cat', message), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_preprocessor VALUES (1, 'see the cat'), (2, 'the see cat'), (3, 'see cat'), (4, 'cat see');

SELECT arraySort(groupArray(id)) FROM tab_preprocessor WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_preprocessor WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '9. Partially materialized index: parts without the index use the same reconstructed expression.';

DROP TABLE IF EXISTS tab_partial;

CREATE TABLE tab_partial (id UInt32, message String)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

SYSTEM STOP MERGES tab_partial;

-- Old part: no index, so the reader evaluates the reconstructed predicate.
INSERT INTO tab_partial VALUES (1, 'see the cat'), (2, 'see cat');

ALTER TABLE tab_partial ADD INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = if(message IN ('the'), '', message), support_phrase_search = 1);

INSERT INTO tab_partial VALUES (3, 'see the cat'), (4, 'see cat');

SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasPhrase(message, 'see cat') SETTINGS query_plan_direct_read_from_text_index = 0;

-- The stop word is not indexed, so it is not searchable in any part: [].
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasToken(message, 'the') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasToken(message, 'the') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasAllTokens(message, ['see', 'cat']) SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasAllTokens(message, ['see', 'cat']) SETTINGS query_plan_direct_read_from_text_index = 0;

ALTER TABLE tab_partial MATERIALIZE INDEX idx;

-- After full materialization the results are unchanged.
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasPhrase(message, 'see cat');
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasToken(message, 'the');
SELECT arraySort(groupArray(id)) FROM tab_partial WHERE hasAllTokens(message, ['see', 'cat']);

SYSTEM START MERGES tab_partial;

DROP TABLE tab_eq;
DROP TABLE tab_in;
DROP TABLE tab_not_in;
DROP TABLE tab_array;
DROP TABLE tab_multi_if;
DROP TABLE tab_preprocessor;
DROP TABLE tab_partial;
