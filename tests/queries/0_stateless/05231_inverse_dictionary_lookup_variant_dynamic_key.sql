-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` is randomized by the test runner, so every query pins it
-- explicitly instead of relying on the session default.
-- Every case materialises the predicate through `WHERE` or `sum()`: a bare `count()` wrapper can hide
-- the defect, because `count(if(cond, NULL, x))` becomes `countIf` and the comparison is never evaluated.

DROP DICTIONARY IF EXISTS dict_keys_123;
DROP DICTIONARY IF EXISTS dict_keys_012;
DROP DICTIONARY IF EXISTS dict_key_2;
DROP DICTIONARY IF EXISTS dict_complex_keys_012;
DROP DICTIONARY IF EXISTS dict_json_key;
DROP DICTIONARY IF EXISTS dict_json_keys_2;
DROP TABLE IF EXISTS src_keys_123;
DROP TABLE IF EXISTS src_keys_012;
DROP TABLE IF EXISTS src_key_2;
DROP TABLE IF EXISTS src_complex_keys_012;
DROP TABLE IF EXISTS src_json_key;
DROP TABLE IF EXISTS src_json_keys_2;
DROP TABLE IF EXISTS data_variant;
DROP TABLE IF EXISTS data_dynamic;
DROP TABLE IF EXISTS data_nullable;
DROP TABLE IF EXISTS data_array_dynamic;
DROP TABLE IF EXISTS data_json;
DROP TABLE IF EXISTS data_json_string;

CREATE TABLE src_keys_123 (k UInt64, a String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_keys_123 VALUES (1, 'x'), (2, 'x'), (3, 'x');

-- Contains key 0, which is what a discriminator NULL converts to.
CREATE TABLE src_keys_012 (k UInt64, a String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_keys_012 VALUES (0, 'x'), (1, 'x'), (2, 'x');

CREATE TABLE src_key_2 (k UInt64, a String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_key_2 VALUES (2, 'x');

CREATE TABLE src_complex_keys_012 (k1 UInt64, k2 String, a String) ENGINE = MergeTree ORDER BY (k1, k2);
INSERT INTO src_complex_keys_012 VALUES (0, 's', 'x'), (1, 's', 'x'), (2, 's', 'x');

CREATE TABLE src_json_key (jk JSON, a String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src_json_key SELECT '{"a":1}'::JSON, 'x';

CREATE TABLE src_json_keys_2 (jk JSON, a String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src_json_keys_2 SELECT '{"a":1}'::JSON, 'x';
INSERT INTO src_json_keys_2 SELECT '{"a":2}'::JSON, 'x';

CREATE DICTIONARY dict_keys_123 (k UInt64, a String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src_keys_123')) LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY dict_keys_012 (k UInt64, a String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src_keys_012')) LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY dict_key_2 (k UInt64, a String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src_key_2')) LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY dict_complex_keys_012 (k1 UInt64, k2 String, a String) PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'src_complex_keys_012')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

CREATE DICTIONARY dict_json_key (jk JSON, a String) PRIMARY KEY jk
SOURCE(CLICKHOUSE(TABLE 'src_json_key')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

CREATE DICTIONARY dict_json_keys_2 (jk JSON, a String) PRIMARY KEY jk
SOURCE(CLICKHOUSE(TABLE 'src_json_keys_2')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

CREATE TABLE data_variant (vk Variant(UInt64, String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_variant SELECT if(number % 10 = 0, NULL, number::Variant(UInt64, String)) FROM numbers(100);

CREATE TABLE data_dynamic (dk Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_dynamic SELECT if(number % 10 = 0, NULL, number::Dynamic) FROM numbers(100);

CREATE TABLE data_nullable (nk Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_nullable SELECT if(number % 10 = 0, NULL, number) FROM numbers(100);

CREATE TABLE data_array_dynamic (adk Array(Dynamic)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_array_dynamic SELECT [if(number % 10 = 0, NULL, number::Dynamic)] FROM numbers(100);

CREATE TABLE data_json (jk JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_json SELECT '{"a":1}'::JSON;

-- The probe is a plain `String`, so only the dictionary's declared key type is dynamic here.
CREATE TABLE data_json_string (sk String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_json_string VALUES ('{"a":1}'), ('{"a":2}');

-- Each case is checked twice: the answer with the rewrite on beside the answer with it off (they must
-- agree), and whether the pass left the `dictGet` call in place.

SELECT 'variant key', (SELECT count() FROM data_variant WHERE dictGet('dict_keys_123', 'a', vk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_variant WHERE dictGet('dict_keys_123', 'a', vk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'variant key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_variant WHERE dictGet('dict_keys_123', 'a', vk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'variant key, dict has key 0', (SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'variant key, dict has key 0, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'variant key, single-key dict', (SELECT sum(dictGet('dict_key_2', 'a', vk) = 'x') FROM data_variant SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT sum(dictGet('dict_key_2', 'a', vk) = 'x') FROM data_variant SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'variant key, single-key dict, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT sum(dictGet('dict_key_2', 'a', vk) = 'x') FROM data_variant SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'variant key, LIKE', (SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0), (SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 0, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0);
SELECT 'variant key, LIKE, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0);

SELECT 'variant key, no dict key matches', (SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) = 'zzz' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) = 'zzz' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'variant key, no dict key matches, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_variant WHERE dictGet('dict_keys_012', 'a', vk) = 'zzz' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'dynamic key', (SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_123', 'a', dk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_123', 'a', dk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'dynamic key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_123', 'a', dk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'dynamic key, dict has key 0', (SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_012', 'a', dk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_012', 'a', dk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'dynamic key, dict has key 0, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_012', 'a', dk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'dynamic key, single-key dict', (SELECT sum(dictGet('dict_key_2', 'a', dk) = 'x') FROM data_dynamic SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT sum(dictGet('dict_key_2', 'a', dk) = 'x') FROM data_dynamic SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'dynamic key, single-key dict, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT sum(dictGet('dict_key_2', 'a', dk) = 'x') FROM data_dynamic SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'dynamic key, LIKE', (SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_012', 'a', dk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0), (SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_012', 'a', dk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 0, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0);
SELECT 'dynamic key, LIKE, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_dynamic WHERE dictGet('dict_keys_012', 'a', dk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0);

SELECT 'dynamic array element key', (SELECT count() FROM data_array_dynamic WHERE dictGet('dict_keys_012', 'a', adk[1]) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_array_dynamic WHERE dictGet('dict_keys_012', 'a', adk[1]) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'dynamic array element key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_array_dynamic WHERE dictGet('dict_keys_012', 'a', adk[1]) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'dynamic in complex key', (SELECT count() FROM data_dynamic WHERE dictGet('dict_complex_keys_012', 'a', (dk, 's')) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_dynamic WHERE dictGet('dict_complex_keys_012', 'a', (dk, 's')) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'dynamic in complex key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_dynamic WHERE dictGet('dict_complex_keys_012', 'a', (dk, 's')) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'variant in complex key', (SELECT count() FROM data_variant WHERE dictGet('dict_complex_keys_012', 'a', (vk, 's')) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_variant WHERE dictGet('dict_complex_keys_012', 'a', (vk, 's')) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'variant in complex key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_variant WHERE dictGet('dict_complex_keys_012', 'a', (vk, 's')) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'variant key in projection', (SELECT sum(dictGet('dict_keys_123', 'a', vk) = 'x') FROM data_variant SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT sum(dictGet('dict_keys_123', 'a', vk) = 'x') FROM data_variant SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'variant key in projection declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT sum(dictGet('dict_keys_123', 'a', vk) = 'x') FROM data_variant SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'json key', (SELECT count() FROM data_json WHERE dictGet('dict_json_key', 'a', jk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_json WHERE dictGet('dict_json_key', 'a', jk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'json key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_json WHERE dictGet('dict_json_key', 'a', jk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

-- `dictGet` accepts a probe that merely converts to the dictionary's key type, so a plainly-typed probe
-- reaches a dictionary whose declared key is `JSON`. The rewrite then compares that probe against keys
-- materialised at the dictionary's key type, which is the same non-equivalence as above.

SELECT 'json declared key, string probe, two keys', (SELECT count() FROM data_json_string WHERE dictGet('dict_json_keys_2', 'a', sk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_json_string WHERE dictGet('dict_json_keys_2', 'a', sk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'json declared key, string probe, two keys, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_json_string WHERE dictGet('dict_json_keys_2', 'a', sk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'json declared key, string probe, one key', (SELECT count() FROM data_json_string WHERE dictGet('dict_json_key', 'a', sk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_json_string WHERE dictGet('dict_json_key', 'a', sk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'json declared key, string probe, one key, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_json_string WHERE dictGet('dict_json_key', 'a', sk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

-- `LIKE` is rewritten through the `IN (SELECT ... FROM dictionary(...))` route, which answers correctly
-- for this shape both before and after the change, so only the decline row below is an oracle: it pins
-- the deliberate decision to screen that route on the declared key type as well.
SELECT 'json declared key, string probe, LIKE', (SELECT count() FROM data_json_string WHERE dictGet('dict_json_key', 'a', sk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0), (SELECT count() FROM data_json_string WHERE dictGet('dict_json_key', 'a', sk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 0, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0);
SELECT 'json declared key, string probe, LIKE, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_json_string WHERE dictGet('dict_json_key', 'a', sk) LIKE 'x' SETTINGS optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 0, optimize_rewrite_like_perfect_affix = 0);

-- A `Nullable` key propagates its NULL through the `dictGet` key conversion, so the rewrite stays
-- equivalent and must keep being applied. Same dictionaries and NULL pattern as the cases above.

SELECT 'nullable key, dict has key 0', (SELECT count() FROM data_nullable WHERE dictGet('dict_keys_012', 'a', nk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_nullable WHERE dictGet('dict_keys_012', 'a', nk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'nullable key, dict has key 0, declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_nullable WHERE dictGet('dict_keys_012', 'a', nk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);

SELECT 'nullable key', (SELECT count() FROM data_nullable WHERE dictGet('dict_keys_123', 'a', nk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1), (SELECT count() FROM data_nullable WHERE dictGet('dict_keys_123', 'a', nk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 0);
SELECT 'nullable key declined', countIf(explain ILIKE '%function_name: dictGet,%') > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM data_nullable WHERE dictGet('dict_keys_123', 'a', nk) = 'x' SETTINGS optimize_inverse_dictionary_lookup = 1);
