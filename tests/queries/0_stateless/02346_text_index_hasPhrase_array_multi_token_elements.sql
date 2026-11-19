-- hasPhrase on Array elements that split into several tokens, and Array phrases whose entries hold several tokens.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;

DROP TABLE IF EXISTS tab_ref;
DROP TABLE IF EXISTS tab_idx;
DROP TABLE IF EXISTS tab_pp_ref;
DROP TABLE IF EXISTS tab_pp_idx;

CREATE TABLE tab_ref
(
    id UInt32,
    messages Array(String)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_idx
(
    id UInt32,
    messages Array(String),
    INDEX idx(messages) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_ref VALUES
    (1, ['the quick brown fox', 'jumps over']),
    (2, ['the quick', 'brown fox']),
    (3, ['quick brown', 'quick brown fox']),
    (4, ['a quick', 'brown fox b']),
    (5, ['fox quick brown', 'fox']),
    (6, ['', 'quick brown fox']),
    (7, ['quick brown fox jumps', 'quick brown fox']),
    (8, []);

INSERT INTO tab_idx SELECT id, messages FROM tab_ref;

-- Three lines per predicate: without an index, through the index, and with the index disabled.
SELECT 'quick brown fox';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'quick brown fox');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'quick brown fox');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'quick brown fox') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'brown fox';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'brown fox') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'fox quick, the last token of an element before the first of the same element';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'fox quick');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'fox quick');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'fox quick') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'fox jumps, adjacent only across elements in row 1';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'fox jumps');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'fox jumps');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'fox jumps') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'quick brown fox jumps over, longer than any element';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, 'quick brown fox jumps over');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'quick brown fox jumps over');
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, 'quick brown fox jumps over') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array phrase of single tokens';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, ['quick', 'brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, ['quick', 'brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, ['quick', 'brown', 'fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array phrase with an entry of two tokens, taken verbatim as one token';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, ['quick brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, ['quick brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, ['quick brown', 'fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array phrase with one entry of three tokens';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, ['quick brown fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, ['quick brown fox']);
SELECT arraySort(groupArray(id)) FROM tab_idx WHERE hasPhrase(messages, ['quick brown fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'the array tokenizer matches a whole element';
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, ['quick brown fox'], 'array');
SELECT arraySort(groupArray(id)) FROM tab_ref WHERE hasPhrase(messages, ['quick brown', 'quick brown fox'], 'array');

CREATE TABLE tab_pp_ref
(
    id UInt32,
    messages Array(String)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_pp_idx
(
    id UInt32,
    messages Array(String),
    INDEX idx(messages) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(messages), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_pp_ref VALUES
    (1, ['The QUICK Brown fox', 'Jumps']),
    (2, ['the Quick', 'BROWN fox']),
    (3, ['QUICK brown', 'Quick Brown Fox']);

INSERT INTO tab_pp_idx SELECT id, messages FROM tab_pp_ref;

SELECT 'quick brown fox with a postprocessor';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), 'quick brown fox');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'quick brown fox');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'quick brown fox') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'brown fox with a postprocessor';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, 'brown fox') SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array phrase of single tokens with a postprocessor';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), ['quick', 'brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, ['quick', 'brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, ['quick', 'brown', 'fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Array phrase with an entry of two tokens with a postprocessor';
SELECT arraySort(groupArray(id)) FROM tab_pp_ref WHERE hasPhrase(arrayMap(x -> lower(x), messages), ['quick brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, ['quick brown', 'fox']);
SELECT arraySort(groupArray(id)) FROM tab_pp_idx WHERE hasPhrase(messages, ['quick brown', 'fox']) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab_ref;
DROP TABLE tab_idx;
DROP TABLE tab_pp_ref;
DROP TABLE tab_pp_idx;
