-- https://github.com/ClickHouse/ClickHouse/issues/117021
-- `String = FixedString(N)` ignores the constant's trailing zero padding, but the index terms were
-- extracted from the padded bytes, so every granule looked unmatched and matching rows disappeared.

DROP TABLE IF EXISTS t_text_index_fixed_string;
CREATE TABLE t_text_index_fixed_string
(
    id UInt32,
    s String,
    INDEX tix s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');

SELECT 'ground truth';
SELECT 'hello' = toFixedString('hello', 10);

SELECT 'text ngrams';
SELECT count() FROM t_text_index_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_text_index_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM t_text_index_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

SELECT 'text splitByNonAlpha';
DROP TABLE IF EXISTS t_text_index_fixed_string_split;
CREATE TABLE t_text_index_fixed_string_split
(
    id UInt32,
    s String,
    INDEX tix s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_fixed_string_split VALUES (1, 'hello'), (2, 'world'), (3, 'hello');
SELECT count() FROM t_text_index_fixed_string_split WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_text_index_fixed_string_split WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;

SELECT 'text array on a String column';
DROP TABLE IF EXISTS t_text_index_array_tokenizer;
CREATE TABLE t_text_index_array_tokenizer
(
    id UInt32,
    s String,
    INDEX tix s TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_array_tokenizer VALUES (1, 'hello'), (2, 'world'), (3, 'hello');
SELECT count() FROM t_text_index_array_tokenizer WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_text_index_array_tokenizer WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_array_tokenizer WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM t_text_index_array_tokenizer WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

SELECT 'text array on a FixedString column';
DROP TABLE IF EXISTS t_text_index_fixed_string_column;
CREATE TABLE t_text_index_fixed_string_column
(
    id UInt32,
    s FixedString(6),
    INDEX tix s TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_fixed_string_column VALUES (1, 'hello'), (2, 'world'), (3, 'hello');
SELECT count() FROM t_text_index_fixed_string_column WHERE s = toFixedString('hello', 6);
SELECT count() FROM t_text_index_fixed_string_column WHERE s = toFixedString('hello', 6) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_fixed_string_column WHERE s IN (SELECT toFixedString('hello', 6));
SELECT count() FROM t_text_index_fixed_string_column WHERE s IN (SELECT toFixedString('hello', 6)) SETTINGS use_skip_indexes = 0;

SELECT 'ngrams on a FixedString column';
DROP TABLE IF EXISTS t_text_index_fixed_string_ngrams;
CREATE TABLE t_text_index_fixed_string_ngrams
(
    id UInt32,
    s FixedString(6),
    INDEX tix s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_fixed_string_ngrams VALUES (1, 'hello'), (2, 'world'), (3, 'hello');
SELECT count() FROM t_text_index_fixed_string_ngrams WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_text_index_fixed_string_ngrams WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;

SELECT 'ngrambf_v1';
DROP TABLE IF EXISTS t_ngrambf_fixed_string;
CREATE TABLE t_ngrambf_fixed_string
(
    id UInt32,
    s String,
    INDEX nix s TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_ngrambf_fixed_string VALUES (1, 'hello'), (2, 'world'), (3, 'hello'), (4, 'foobar');
SELECT count() FROM t_ngrambf_fixed_string WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_ngrambf_fixed_string WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngrambf_fixed_string WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM t_ngrambf_fixed_string WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

SELECT 'sparseGrams on a FixedString column';
DROP TABLE IF EXISTS t_text_index_sparse_grams;
CREATE TABLE t_text_index_sparse_grams
(
    id UInt32,
    s FixedString(6),
    INDEX tix s TYPE text(tokenizer = sparseGrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_sparse_grams VALUES (1, 'hello'), (2, 'world'), (3, 'hello');
SELECT count() FROM t_text_index_sparse_grams WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_text_index_sparse_grams WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_sparse_grams WHERE s IN (SELECT toFixedString('hello', 10));
SELECT count() FROM t_text_index_sparse_grams WHERE s IN (SELECT toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0;

SELECT 'hasAny and hasAll ignore the padding too';
DROP TABLE IF EXISTS t_text_index_array_column;
CREATE TABLE t_text_index_array_column
(
    id UInt32,
    arr Array(String),
    INDEX tix arr TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_array_column VALUES (1, ['hello']), (2, ['world']), (3, ['hello']);
SELECT count() FROM t_text_index_array_column WHERE hasAny(arr, [toFixedString('hello', 10)]);
SELECT count() FROM t_text_index_array_column WHERE hasAny(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_array_column WHERE hasAll(arr, [toFixedString('hello', 10)]);
SELECT count() FROM t_text_index_array_column WHERE hasAll(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE IF EXISTS t_ngrambf_array_column;
CREATE TABLE t_ngrambf_array_column
(
    id UInt32,
    arr Array(String),
    INDEX nix arr TYPE ngrambf_v1(3, 512, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_ngrambf_array_column VALUES (1, ['hello']), (2, ['world']), (3, ['hello']);
SELECT count() FROM t_ngrambf_array_column WHERE hasAny(arr, [toFixedString('hello', 10)]);
SELECT count() FROM t_ngrambf_array_column WHERE hasAny(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ngrambf_array_column WHERE hasAll(arr, [toFixedString('hello', 10)]);
SELECT count() FROM t_ngrambf_array_column WHERE hasAll(arr, [toFixedString('hello', 10)]) SETTINGS use_skip_indexes = 0;

-- These compare the raw padded bytes, so their terms must keep the padding. `text(tokenizer = array)`
-- answers them by exact direct read, where a stripped term would return rows the predicate rejects.
SELECT 'has and mapContains keep the padding';
SELECT count() FROM t_text_index_array_column WHERE has(arr, toFixedString('hello', 10));
SELECT count() FROM t_text_index_array_column WHERE has(arr, toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_array_column WHERE has(arr, 'hello');
SELECT count() FROM t_text_index_array_column WHERE has(arr, 'hello') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE IF EXISTS t_text_index_map_keys;
CREATE TABLE t_text_index_map_keys
(
    id UInt32,
    m Map(String, String),
    INDEX kix mapKeys(m) TYPE text(tokenizer = array),
    INDEX vix mapValues(m) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO t_text_index_map_keys VALUES (1, map('hello', 'world')), (2, map('foo', 'bar')), (3, map('hello', 'world'));
SELECT count() FROM t_text_index_map_keys WHERE mapContainsKey(m, toFixedString('hello', 10));
SELECT count() FROM t_text_index_map_keys WHERE mapContainsKey(m, toFixedString('hello', 10)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_map_keys WHERE mapContainsValue(m, toFixedString('world', 10));
SELECT count() FROM t_text_index_map_keys WHERE mapContainsValue(m, toFixedString('world', 10)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_map_keys WHERE mapContainsKey(m, 'hello');
SELECT count() FROM t_text_index_map_keys WHERE mapContainsKey(m, 'hello') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'startsWith and endsWith keep the padding';
SELECT count() FROM t_text_index_fixed_string WHERE startsWith(s, toFixedString('hel', 10));
SELECT count() FROM t_text_index_fixed_string WHERE startsWith(s, toFixedString('hel', 10)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_fixed_string WHERE endsWith(s, toFixedString('llo', 10));
SELECT count() FROM t_text_index_fixed_string WHERE endsWith(s, toFixedString('llo', 10)) SETTINGS use_skip_indexes = 0;

SELECT 'unpadded constant still prunes';
SELECT count() FROM t_text_index_fixed_string WHERE s = 'hello';
SELECT count() FROM t_text_index_fixed_string WHERE s = 'nosuch';
SELECT count() FROM t_ngrambf_fixed_string WHERE s = 'hello';
SELECT count() FROM t_ngrambf_fixed_string WHERE s = 'nosuch';

DROP TABLE t_text_index_fixed_string;
DROP TABLE t_text_index_fixed_string_split;
DROP TABLE t_text_index_array_tokenizer;
DROP TABLE t_text_index_fixed_string_column;
DROP TABLE t_text_index_fixed_string_ngrams;
DROP TABLE t_ngrambf_fixed_string;
DROP TABLE t_text_index_sparse_grams;
DROP TABLE t_text_index_array_column;
DROP TABLE t_ngrambf_array_column;
DROP TABLE t_text_index_map_keys;
