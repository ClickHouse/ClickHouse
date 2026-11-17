-- Tests `hasPhrase`, `hasAnyTokens`, and `hasAllTokens` on JSON paths covered by a `JSONAllValues` text index.
-- See issue https://github.com/ClickHouse/ClickHouse/issues/123427.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET query_plan_direct_read_from_text_index = 1;

CREATE TABLE tab_split_by_string
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_split_by_non_alpha
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_ngrams
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_typed
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_array
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_split_by_string
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_separator
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_postprocessor
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_preprocessor
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']), preprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_regexp_preprocessor
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']), preprocessor = replaceRegexpAll(JSONAllValues(data), 'QUICK', 'fast'))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_sparse_grams
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = sparseGrams(3, 5))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_case_mapping
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_case_mapping_letter_separator
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString(['X']), preprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_case_mapping_postprocessor
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(JSONAllValues(data)), postprocessor = replaceAll(JSONAllValues(data), 'B', 'a'))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_phrase_preprocessor
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, preprocessor = replaceAll(JSONAllValues(data), 'a b', 'c'))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_has_token_array_tokenizer
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = array, preprocessor = lower(JSONAllValues(data)), postprocessor = replaceAll(JSONAllValues(data), 'quick brown', 'zebra'))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_has_token_separator
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, postprocessor = replaceAll(JSONAllValues(data), ' ', ''))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_split_by_string VALUES (1, '{"key": "the quick brown fox"}'), (2, '{"key": "lazy dog"}'), (3, '{"key": "brown quick"}');
INSERT INTO tab_split_by_non_alpha SELECT * FROM tab_split_by_string;
INSERT INTO tab_array SELECT * FROM tab_split_by_string;
INSERT INTO tab_ngrams SELECT * FROM tab_split_by_string;
INSERT INTO tab_typed SELECT * FROM tab_split_by_string;
INSERT INTO tab_array_path_array VALUES (1, '{"key": ["the quick brown fox", "cat"]}'), (2, '{"key": ["lazy", "dog"]}');
INSERT INTO tab_array_path_split_by_string SELECT * FROM tab_array_path_array;
INSERT INTO tab_array_path_separator VALUES (1, '{"key": ["the quick()brown fox"]}'), (2, '{"key": ["lazy dog"]}');
INSERT INTO tab_preprocessor VALUES (1, '{"key": "QUICK BROWN fox"}'), (2, '{"key": "lazy dog"}');
INSERT INTO tab_regexp_preprocessor SELECT * FROM tab_preprocessor;
INSERT INTO tab_array_path_sparse_grams VALUES (1, '{"key": ["quick brown"]}'), (2, '{"key": ["lazy dog"]}');
INSERT INTO tab_case_mapping SELECT * FROM tab_preprocessor;
INSERT INTO tab_case_mapping_letter_separator VALUES (1, '{"key": "aXb"}'), (2, '{"key": "cXd"}');
INSERT INTO tab_case_mapping_postprocessor VALUES (1, '{"key": "a"}'), (2, '{"key": "c"}');
INSERT INTO tab_phrase_preprocessor VALUES (1, '{"key": "a b"}'), (2, '{"key": "d"}');
INSERT INTO tab_has_token_array_tokenizer VALUES (1, '{"key": "quick brown"}'), (2, '{"key": "lazy dog"}');
INSERT INTO tab_has_token_separator VALUES (1, '{"key": "a b"}');
INSERT INTO tab_array_path_postprocessor VALUES (1, '{"key": ["CAT"]}'), (2, '{"key": ["dog"]}');

SELECT 'splitByString';
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, 'quick brown') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, 'quick brown') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'splitByString, index condition';
SELECT trim(explain) FROM (
  EXPLAIN indexes = 1
  SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'brown'])
)
WHERE explain LIKE '%tokens:%'
SETTINGS enable_parallel_replicas = 0;

SELECT 'splitByNonAlpha';
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_non_alpha WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_non_alpha WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array';
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['quick', 'brown'], 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['quick', 'brown'], 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['the quick brown fox'], 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['the quick brown fox'], 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'ngrams';
SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasPhrase(data.key::String, ['qui', 'uic', 'ick'], 'ngrams(3)') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasPhrase(data.key::String, ['qui', 'uic', 'ick'], 'ngrams(3)') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'typed path';
SELECT groupArray(id) FROM (SELECT id FROM tab_typed WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_typed WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array-valued path, array';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, ['the quick brown fox'], 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, ['the quick brown fox'], 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, 'dog', 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, 'dog', 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAllTokens(data.key, ['the quick brown fox', 'cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAllTokens(data.key, ['the quick brown fox', 'cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, 'dog') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, 'dog') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array-valued path, splitByString';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, 'quick brown') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, 'quick brown') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, ['quick', 'cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, ['quick', 'cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, 'quick cat') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, 'quick cat') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array-valued path, index tokenizer is applied';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAnyTokens(data.key, ['quick()brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAnyTokens(data.key, ['quick()brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAllTokens(data.key, ['quick()brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAllTokens(data.key, ['quick()brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasPhrase(data.key, ['quick()brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasPhrase(data.key, ['quick()brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(r) FROM (SELECT hasPhrase(data.key, ['quick()brown']) AS r FROM tab_array_path_separator ORDER BY id);

SELECT 'array-valued path, index postprocessor is applied';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAnyTokens(data.key, ['cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAnyTokens(data.key, ['cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAllTokens(data.key, ['cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAllTokens(data.key, ['cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasPhrase(data.key, ['cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasPhrase(data.key, ['cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(r) FROM (SELECT hasAnyTokens(data.key, ['cat']) AS r FROM tab_array_path_postprocessor ORDER BY id);

SELECT 'preprocessor, array needles';
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAnyTokens(data.key, ['QUICK']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAnyTokens(data.key, ['QUICK']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAllTokens(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAllTokens(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, 'QUICK BROWN') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, 'QUICK BROWN') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'case mapping preprocessor, index condition';
SELECT trim(explain) FROM (
  EXPLAIN indexes = 1
  SELECT id FROM tab_case_mapping WHERE hasAnyTokens(data.key, ['QUICK', 'CAT'])
)
WHERE explain LIKE '%tokens:%'
SETTINGS enable_parallel_replicas = 0;

SELECT 'case mapping preprocessor, array needles';
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping WHERE hasAnyTokens(data.key, ['QUICK', 'CAT']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping WHERE hasAnyTokens(data.key, ['QUICK', 'CAT']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping WHERE hasAllTokens(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping WHERE hasAllTokens(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'case mapping preprocessor, tokenizer with a letter separator';
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_letter_separator WHERE hasPhrase(data.key, ['a']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_letter_separator WHERE hasPhrase(data.key, ['a']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_letter_separator WHERE hasAnyTokens(data.key, ['a']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_letter_separator WHERE hasAnyTokens(data.key, ['a']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_letter_separator WHERE hasAllTokens(data.key, ['a', 'b']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_letter_separator WHERE hasAllTokens(data.key, ['a', 'b']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'case mapping preprocessor with a postprocessor';
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_postprocessor WHERE hasPhrase(data.key, ['B']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_postprocessor WHERE hasPhrase(data.key, ['B']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_postprocessor WHERE hasAnyTokens(data.key, ['B']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_postprocessor WHERE hasAnyTokens(data.key, ['B']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_postprocessor WHERE hasAllTokens(data.key, ['B']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_case_mapping_postprocessor WHERE hasAllTokens(data.key, ['B']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'other preprocessor, array needles';
SELECT groupArray(id) FROM (SELECT id FROM tab_regexp_preprocessor WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_regexp_preprocessor WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_regexp_preprocessor WHERE hasAnyTokens(data.key, ['QUICK']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_regexp_preprocessor WHERE hasAnyTokens(data.key, ['QUICK']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'other preprocessor, string needles';
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasPhrase(data.key, 'a') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasPhrase(data.key, 'a') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasAnyTokens(data.key, 'a') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasAnyTokens(data.key, 'a') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasAllTokens(data.key, 'a') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasAllTokens(data.key, 'a') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasToken(data.key, 'a') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasToken(data.key, 'a') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasTokenOrNull(data.key, 'a') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasTokenOrNull(data.key, 'a') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasPhrase(data.key, ['a']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_phrase_preprocessor WHERE hasPhrase(data.key, ['a']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'hasToken checks are kept when the index cannot prune';
SELECT groupArray(id) FROM (SELECT id FROM tab_has_token_array_tokenizer WHERE hasToken(data.key, 'quick') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_has_token_array_tokenizer WHERE hasToken(data.key, 'quick') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab_has_token_separator WHERE hasToken(data.key, 'a b'); -- { serverError BAD_ARGUMENTS }

SELECT 'array-valued path, tokenizer unsupported by hasPhrase';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_sparse_grams WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_sparse_grams WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_sparse_grams WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab_split_by_string;
DROP TABLE tab_split_by_non_alpha;
DROP TABLE tab_array;
DROP TABLE tab_ngrams;
DROP TABLE tab_typed;
DROP TABLE tab_array_path_array;
DROP TABLE tab_array_path_split_by_string;
DROP TABLE tab_array_path_separator;
DROP TABLE tab_array_path_postprocessor;
DROP TABLE tab_preprocessor;
DROP TABLE tab_case_mapping;
DROP TABLE tab_case_mapping_letter_separator;
DROP TABLE tab_case_mapping_postprocessor;
DROP TABLE tab_regexp_preprocessor;
DROP TABLE tab_phrase_preprocessor;
DROP TABLE tab_has_token_array_tokenizer;
DROP TABLE tab_has_token_separator;
DROP TABLE tab_array_path_sparse_grams;
