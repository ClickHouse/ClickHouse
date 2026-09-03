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
