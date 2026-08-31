-- https://github.com/ClickHouse/ClickHouse/issues/117021
-- `String = FixedString(N)` ignores the constant's trailing zero padding, but the index terms were
-- extracted from the padded bytes: for an n-gram tokenizer that yields n-grams containing `\0`, which
-- never occur in the stored data, so every granule was pruned and matching rows silently disappeared.

DROP TABLE IF EXISTS t_text_index_fixed_string;
CREATE TABLE t_text_index_fixed_string (id UInt32, s String, INDEX tix s TYPE text(tokenizer = ngrams(3)) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
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
CREATE TABLE t_text_index_fixed_string_split (id UInt32, s String, INDEX tix s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_text_index_fixed_string_split VALUES (1, 'hello'), (2, 'world'), (3, 'hello');
SELECT count() FROM t_text_index_fixed_string_split WHERE s = toFixedString('hello', 10);
SELECT count() FROM t_text_index_fixed_string_split WHERE s = toFixedString('hello', 10) SETTINGS use_skip_indexes = 0;

SELECT 'ngrambf_v1';
DROP TABLE IF EXISTS t_ngrambf_fixed_string;
CREATE TABLE t_ngrambf_fixed_string (id UInt32, s String, INDEX nix s TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
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
DROP TABLE t_ngrambf_fixed_string;
