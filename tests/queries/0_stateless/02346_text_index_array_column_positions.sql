-- A text index on an Array column answers the token predicates exactly as the same data without an index,
-- and hasPhrase matches a phrase only within one element, with and without the index.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;

SELECT 'Array column';

CREATE TABLE tab_ref (
    id UInt32,
    messages Array(String)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_idx (
    id UInt32,
    messages Array(String),
    INDEX idx(messages) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_ref VALUES
    (1, ['quick brown', 'fox jumps']),
    (2, ['brown fox', '']),
    (3, []),
    (4, ['quick', 'fox']),
    (5, ['lazy dog']);

INSERT INTO tab_idx SELECT id, messages FROM tab_ref;

-- Three lines per predicate: without an index, through the index, and with the index disabled.
SELECT 'hasAllTokens quick fox';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasAllTokens(messages, ['quick', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasAllTokens(messages, ['quick', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasAllTokens(messages, ['quick', 'fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasAnyTokens jumps dog';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasAnyTokens(messages, ['jumps', 'dog']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasAnyTokens(messages, ['jumps', 'dog']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasAnyTokens(messages, ['jumps', 'dog']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasAllTokens brown';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasAllTokens(messages, ['brown']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasAllTokens(messages, ['brown']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasAllTokens(messages, ['brown']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array column with a postprocessor';

CREATE TABLE tab_pp_ref (
    id UInt32,
    messages Array(String)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_pp_idx (
    id UInt32,
    messages Array(String),
    INDEX idx(messages) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(messages), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_pp_ref VALUES
    (1, ['Quick BROWN', 'Fox']),
    (2, ['brown fox']),
    (3, ['LAZY', 'dog']);

INSERT INTO tab_pp_idx SELECT id, messages FROM tab_pp_ref;

SELECT 'hasAllTokens quick fox, lowercased';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasAllTokens(lower(arrayStringConcat(messages, ' ')), ['quick', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasAllTokens(messages, ['quick', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasAllTokens(messages, ['quick', 'fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasAnyTokens lazy, lowercased';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasAnyTokens(lower(arrayStringConcat(messages, ' ')), ['lazy']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasAnyTokens(messages, ['lazy']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasAnyTokens(messages, ['lazy']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase quick brown, lowercased';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), 'quick brown');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'quick brown');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'quick brown') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase brown fox, lowercased';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'brown fox') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase lazy dog, lowercased';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), 'lazy dog');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'lazy dog');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'lazy dog') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array column with the array tokenizer and a postprocessor';

CREATE TABLE tab_pp_arr_ref (
    id UInt32,
    messages Array(String)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_pp_arr_idx (
    id UInt32,
    messages Array(String),
    INDEX idx(messages) TYPE text(tokenizer = array, postprocessor = lower(messages), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_pp_arr_ref VALUES (1, ['Hello', 'World']), (2, ['world', 'HELLO']);
INSERT INTO tab_pp_arr_idx SELECT id, messages FROM tab_pp_arr_ref;

SELECT 'hasPhrase hello world spans elements';
SELECT arraySort(groupArray(id)) FROM tab_pp_arr_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), ['hello', 'world'], 'array');
SELECT arraySort(groupArray(id)) FROM tab_pp_arr_idx WHERE hasPhrase(messages, ['hello', 'world']);
SELECT arraySort(groupArray(id)) FROM tab_pp_arr_idx WHERE hasPhrase(messages, ['hello', 'world']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab_pp_arr_ref;
DROP TABLE tab_pp_arr_idx;

SELECT 'Array column with a preprocessor and a postprocessor';

CREATE TABLE tab_prepp_ref (
    id UInt32,
    messages Array(String)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_prepp_idx (
    id UInt32,
    messages Array(String),
    INDEX idx(messages) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(messages), postprocessor = if(messages = 'bar', '', messages), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_prepp_ref VALUES (1, ['Hello Bar World', 'x']), (2, ['HELLO', 'world']), (3, ['hello', 'bar world']);
INSERT INTO tab_prepp_idx SELECT id, messages FROM tab_prepp_ref;

SELECT 'hasPhrase hello world, a dropped token does not break a phrase within one element';
SELECT arraySort(groupArray(id)) FROM tab_prepp_ref WHERE hasPhrase(arrayMap(x -> replaceAll(lower(x), 'bar', ''), messages), 'hello world');
SELECT arraySort(groupArray(id)) FROM tab_prepp_idx WHERE hasPhrase(messages, 'hello world');
SELECT arraySort(groupArray(id)) FROM tab_prepp_idx WHERE hasPhrase(messages, 'hello world') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab_prepp_ref;
DROP TABLE tab_prepp_idx;

SELECT 'hasPhrase on an Array column';

INSERT INTO tab_ref VALUES (6, ['hello, clickhouse', 'happy, world']), (7, ['hello world']), (8, ['clickhouse, hello', 'world, happy']);
INSERT INTO tab_idx SELECT id, messages FROM tab_ref WHERE id >= 6;

SELECT 'hasPhrase hello world';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'hello world');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'hello world');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'hello world') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase brown fox';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'brown fox') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab_ref;
DROP TABLE tab_idx;
DROP TABLE tab_pp_ref;
DROP TABLE tab_pp_idx;
