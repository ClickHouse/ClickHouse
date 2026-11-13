-- `position_increment_gap` separates the elements of an `Array` row, so `hasPhrase` is exact and does not span elements.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_gap;
DROP TABLE IF EXISTS tab_lc;
DROP TABLE IF EXISTS tab_pp;
DROP TABLE IF EXISTS tab_string;
DROP TABLE IF EXISTS tab_array_tokenizer;
DROP TABLE IF EXISTS tab_err;

CREATE TABLE tab
(
    id UInt32,
    arr Array(Nullable(String)),
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab VALUES
    (1, ['hello, clickhouse', 'happy, world']),
    (2, ['hello world']),
    (3, ['clickhouse, hello', 'world, happy']),
    (4, ['hello', '', NULL, 'world']);

SELECT 'positions with the default gap, empty and NULL elements add no gap';
SELECT row_id, token, positions FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY row_id, token;

SELECT 'hasPhrase hello world';
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'hello world') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'hello world') ORDER BY id) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase clickhouse happy';
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'clickhouse happy') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'clickhouse happy') ORDER BY id) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase world happy';
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'world happy') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'world happy') ORDER BY id) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase is read from the index, without the function';
SELECT explain LIKE '%hasPhrase(%' FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(arr, 'hello world')) WHERE explain LIKE '%Prewhere filter column%';

SELECT 'positions after a merge';
INSERT INTO tab VALUES (5, ['clickhouse', 'happy']);
OPTIMIZE TABLE tab FINAL;
SELECT row_id, token, positions FROM mergeTreeTextIndex(currentDatabase(), tab, idx) WHERE token IN ('clickhouse', 'happy') ORDER BY row_id, token;
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasPhrase(arr, 'clickhouse happy') ORDER BY id);

SELECT 'an explicit gap';
CREATE TABLE tab_gap
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, position_increment_gap = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_gap VALUES (1, ['hello, clickhouse', 'happy, world']);

SELECT row_id, token, positions FROM mergeTreeTextIndex(currentDatabase(), tab_gap, idx) ORDER BY row_id, token;
SELECT count() FROM tab_gap WHERE hasPhrase(arr, 'clickhouse happy');
SELECT count() FROM tab_gap WHERE hasPhrase(arr, 'happy world');

SELECT 'LowCardinality elements, with and without the cache of tokenized values';
CREATE TABLE tab_lc
(
    id UInt32,
    arr Array(LowCardinality(String)),
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_lc SELECT number, ['quick brown', 'fox'] FROM numbers(16);
INSERT INTO tab_lc VALUES (100, ['quick brown', 'fox']);

SELECT token, groupUniqArray(positions) FROM mergeTreeTextIndex(currentDatabase(), tab_lc, idx) GROUP BY token ORDER BY token;
SELECT count() FROM tab_lc WHERE hasPhrase(arr, 'brown fox');
SELECT count() FROM tab_lc WHERE hasPhrase(arr, 'quick brown');

SELECT 'a postprocessor';
CREATE TABLE tab_pp
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(arr), support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_pp VALUES (1, ['Quick BROWN', '', 'Fox']), (2, ['brown fox']);

SELECT row_id, token, positions FROM mergeTreeTextIndex(currentDatabase(), tab_pp, idx) ORDER BY row_id, token;
SELECT groupArray(id) FROM (SELECT id FROM tab_pp WHERE hasPhrase(arr, 'BROWN fox') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_pp WHERE hasPhrase(arr, 'BROWN fox') ORDER BY id) SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'a String column has no elements to separate';
CREATE TABLE tab_string
(
    id UInt32,
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, position_increment_gap = 5)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_string VALUES (1, 'hello clickhouse world');

SELECT row_id, token, positions FROM mergeTreeTextIndex(currentDatabase(), tab_string, idx) ORDER BY row_id, token;

SELECT 'the array tokenizer: every element is one token and a phrase spans elements';
CREATE TABLE tab_array_tokenizer
(
    id UInt32,
    arr Array(String),
    INDEX idx(arr) TYPE text(tokenizer = array, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_array_tokenizer VALUES (1, ['hello', 'clickhouse', 'world']);

SELECT row_id, token, positions FROM mergeTreeTextIndex(currentDatabase(), tab_array_tokenizer, idx) ORDER BY row_id, token;
SELECT count() FROM tab_array_tokenizer WHERE hasPhrase(arr, ['hello', 'clickhouse']);
SELECT count() FROM tab_array_tokenizer WHERE hasPhrase(arr, ['hello', 'world']);

SELECT 'invalid arguments';
CREATE TABLE tab_err (arr Array(String), INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, position_increment_gap = 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_err (arr Array(String), INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, position_increment_gap = 128)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_err (arr Array(String), INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, position_increment_gap = 1)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_err (arr Array(String), INDEX idx(arr) TYPE text(tokenizer = array, support_phrase_search = 1, position_increment_gap = 1)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_err (arr Array(String), INDEX idx(arr) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, position_increment_gap = 127)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_text_index_phrase_search = 1;

DROP TABLE tab;
DROP TABLE tab_gap;
DROP TABLE tab_lc;
DROP TABLE tab_pp;
DROP TABLE tab_string;
DROP TABLE tab_array_tokenizer;
DROP TABLE tab_err;
