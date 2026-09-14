-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

SET optimize_or_like_chain = 0;
SET optimize_rewrite_like_perfect_affix = 0;

DROP DICTIONARY IF EXISTS dict_int_default_minus1;
DROP DICTIONARY IF EXISTS dict_int_nullable_default_minus1;
DROP DICTIONARY IF EXISTS dict_string;
DROP DICTIONARY IF EXISTS dict_complex_str;
DROP DICTIONARY IF EXISTS dict_nullable_attr_stored;
DROP DICTIONARY IF EXISTS dict_complex_nullable_dict_key;
DROP DICTIONARY IF EXISTS dict_set_limit;
DROP DICTIONARY IF EXISTS dict_ops;
DROP DICTIONARY IF EXISTS dict_flat;
DROP DICTIONARY IF EXISTS dict_hashed;
DROP DICTIONARY IF EXISTS dict_hashed_array;
DROP DICTIONARY IF EXISTS dict_sparse_hashed;
DROP DICTIONARY IF EXISTS dict_complex;
DROP DICTIONARY IF EXISTS dict_complex_array;
DROP DICTIONARY IF EXISTS dict_complex_sparse;
DROP TABLE IF EXISTS ref_int;
DROP TABLE IF EXISTS ref_int_nullable;
DROP TABLE IF EXISTS ref_str;
DROP TABLE IF EXISTS ref_complex_str;
DROP TABLE IF EXISTS ref_nullable_attr_stored;
DROP TABLE IF EXISTS ref_complex_nullable_dict_key;
DROP TABLE IF EXISTS ref_set_limit;
DROP TABLE IF EXISTS ref_ops;
DROP TABLE IF EXISTS ref_layouts_simple;
DROP TABLE IF EXISTS ref_layouts_complex;
DROP TABLE IF EXISTS data_int;
DROP TABLE IF EXISTS data_str;
DROP TABLE IF EXISTS data_str_nullable;
DROP TABLE IF EXISTS data_str_lc_nullable;
DROP TABLE IF EXISTS data_complex;
DROP TABLE IF EXISTS data_complex_lc_nullable;
DROP TABLE IF EXISTS data_complex_single_lc;
DROP TABLE IF EXISTS data_complex_nullable_dict_key;
DROP TABLE IF EXISTS data_outer_nullable_tuple;
DROP TABLE IF EXISTS data_set_limit;
DROP TABLE IF EXISTS data_ops;

CREATE TABLE ref_int (`to` UInt64, `from` Int32) ENGINE = MergeTree ORDER BY `to`;
INSERT INTO ref_int VALUES (42, 9289150);

CREATE TABLE ref_int_nullable (`to` UInt64, `from` Nullable(Int32)) ENGINE = MergeTree ORDER BY `to`;
INSERT INTO ref_int_nullable VALUES (42, 9289150);

CREATE TABLE ref_str (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_str VALUES (1, 'abc');

CREATE DICTIONARY dict_int_default_minus1 (`to` UInt64, `from` Int32 DEFAULT -1)
PRIMARY KEY `to` SOURCE(CLICKHOUSE(TABLE 'ref_int')) LAYOUT(HASHED()) LIFETIME(0);

CREATE DICTIONARY dict_int_nullable_default_minus1 (`to` UInt64, `from` Int32 DEFAULT -1)
PRIMARY KEY `to` SOURCE(CLICKHOUSE(TABLE 'ref_int_nullable')) LAYOUT(HASHED()) LIFETIME(0);

CREATE DICTIONARY dict_string (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_str')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE data_int (id Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_int VALUES (53);

CREATE TABLE data_str (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_str VALUES (1), (2), (3);

CREATE TABLE data_str_nullable (id Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_str_nullable VALUES (1), (2), (NULL);

CREATE TABLE data_str_lc_nullable (id LowCardinality(Nullable(UInt64))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO data_str_lc_nullable VALUES (1), (2), (NULL);


SELECT 'dictGet = default, missing key, no rewrite (default -1 = -1) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, dictGet('dict_int_default_minus1', 'from', toUInt64(id)) AS a, a = -1 AS pred FROM data_int ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet = default, missing key, no rewrite (default -1 = -1)';
SELECT id, dictGet('dict_int_default_minus1', 'from', toUInt64(id)) AS a, a = -1 AS pred FROM data_int ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet = default, Nullable source, no rewrite (default -1 = -1) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, dictGet('dict_int_nullable_default_minus1', 'from', toUInt64(id)) AS a, a = -1 AS pred FROM data_int ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet = default, Nullable source, no rewrite (default -1 = -1)';
SELECT id, dictGet('dict_int_nullable_default_minus1', 'from', toUInt64(id)) AS a, a = -1 AS pred FROM data_int ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetOrNull = const, no rewrite (dictGetOrNull is not supported by the optimization) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, dictGetOrNull('dict_string', 'name', id) = 'abc' AS pred FROM data_str ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGetOrNull = const, no rewrite (dictGetOrNull is not supported by the optimization)';
SELECT id, dictGetOrNull('dict_string', 'name', id) = 'abc' AS pred FROM data_str ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet != non-default, no rewrite (default empty string != abc) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_str WHERE dictGet('dict_string', 'name', id) != 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet != non-default, no rewrite (default empty string != abc)';
SELECT count() FROM data_str WHERE dictGet('dict_string', 'name', id) != 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet LIKE %, no rewrite (default empty string LIKE %) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_str WHERE dictGet('dict_string', 'name', id) LIKE '%'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictGet LIKE %, no rewrite (default empty string LIKE %)';
SELECT count() FROM data_str WHERE dictGet('dict_string', 'name', id) LIKE '%'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Nullable key, dictGetString != abc, no rewrite (default empty string != abc) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_str_nullable WHERE dictGetString('dict_string', 'name', id) != 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Nullable key, dictGetString != abc, no rewrite (default empty string != abc)';
SELECT count() FROM data_str_nullable WHERE dictGetString('dict_string', 'name', id) != 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'LC(Nullable) simple key, dictGetString != abc, no rewrite (default empty string != abc) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_str_lc_nullable WHERE dictGetString('dict_string', 'name', id) != 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'LC(Nullable) simple key, dictGetString != abc, no rewrite (default empty string != abc)';
SELECT count() FROM data_str_lc_nullable WHERE dictGetString('dict_string', 'name', id) != 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

CREATE TABLE ref_complex_str (k1 UInt64, k2 String, name String) ENGINE = MergeTree ORDER BY (k1, k2);
INSERT INTO ref_complex_str VALUES (1, 'a', 'abc');
CREATE DICTIONARY dict_complex_str (k1 UInt64, k2 String, name String)
PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'ref_complex_str')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE data_complex (k1 Nullable(UInt64), k2 String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_complex VALUES (1, 'a'), (2, 'a');

SELECT 'Composite key, Nullable component, no rewrite (skip to preserve dictGet throw on NULL component) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, dictGetString('dict_complex_str', 'name', (k1, k2)) = 'abc' AS pred FROM data_complex ORDER BY k1
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Composite key, Nullable component, no rewrite (skip to preserve dictGet throw on NULL component)';
SELECT k1, dictGetString('dict_complex_str', 'name', (k1, k2)) = 'abc' AS pred FROM data_complex ORDER BY k1
SETTINGS optimize_inverse_dictionary_lookup = 1;

CREATE TABLE data_complex_lc_nullable (k1 LowCardinality(Nullable(UInt64)), k2 String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO data_complex_lc_nullable VALUES (1, 'a'), (2, 'a');

SELECT 'Composite key, LC(Nullable) component, no rewrite (skip to preserve throw) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, dictGetString('dict_complex_str', 'name', (k1, k2)) = 'abc' AS pred FROM data_complex_lc_nullable ORDER BY k1
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Composite key, LC(Nullable) component, no rewrite (skip to preserve throw)';
SELECT k1, dictGetString('dict_complex_str', 'name', (k1, k2)) = 'abc' AS pred FROM data_complex_lc_nullable ORDER BY k1
SETTINGS optimize_inverse_dictionary_lookup = 1;

CREATE TABLE ref_complex_nullable_dict_key (k1 Nullable(UInt64), k2 String, name String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ref_complex_nullable_dict_key VALUES (NULL, 'x', 'nullhit'), (1, 'x', 'one');
CREATE DICTIONARY dict_complex_nullable_dict_key (k1 Nullable(UInt64), k2 String, name String)
PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'ref_complex_nullable_dict_key')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE data_complex_nullable_dict_key (k1 Nullable(UInt64), k2 String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_complex_nullable_dict_key VALUES (NULL, 'x'), (1, 'x'), (2, 'x');

SELECT 'Composite key, dict declares Nullable key, Nullable data, no rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_complex_nullable_dict_key WHERE dictGetString('dict_complex_nullable_dict_key', 'name', (k1, k2)) = 'nullhit'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Composite key, dict declares Nullable key, Nullable data, no rewrite';
SELECT count() FROM data_complex_nullable_dict_key WHERE dictGetString('dict_complex_nullable_dict_key', 'name', (k1, k2)) = 'nullhit'
SETTINGS optimize_inverse_dictionary_lookup = 1;

CREATE TABLE data_outer_nullable_tuple (t Nullable(Tuple(Nullable(UInt64), String))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS allow_experimental_nullable_tuple_type = 1;
INSERT INTO data_outer_nullable_tuple VALUES ((NULL, 'x')), ((1, 'x'));

SELECT 'Nullable(Tuple(Nullable(K))), no rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_outer_nullable_tuple WHERE dictGetString('dict_complex_nullable_dict_key', 'name', t) = 'nullhit'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Nullable(Tuple(Nullable(K))), no rewrite';
SELECT count() FROM data_outer_nullable_tuple WHERE dictGetString('dict_complex_nullable_dict_key', 'name', t) = 'nullhit'
SETTINGS optimize_inverse_dictionary_lookup = 1;

CREATE TABLE ref_nullable_attr_stored (id UInt64, name Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_nullable_attr_stored VALUES (1, 'abc'), (2, NULL);
CREATE DICTIONARY dict_nullable_attr_stored (id UInt64, name Nullable(String) DEFAULT 'missing')
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_nullable_attr_stored')) LAYOUT(HASHED()) LIFETIME(0);

SELECT 'Nullable attr with stored NULL, isNull(predicate), no rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id FROM data_str WHERE isNull(dictGet('dict_nullable_attr_stored', 'name', id) = 'abc') ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Nullable attr with stored NULL, isNull(predicate), no rewrite';
SELECT id FROM data_str WHERE isNull(dictGet('dict_nullable_attr_stored', 'name', id) = 'abc') ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

-- Per-operator no-rewrite examples (default = '' for String, default = 0 for UInt64)
CREATE TABLE ref_ops (id UInt64, name String, n UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_ops VALUES (1, 'apple', 5);
CREATE DICTIONARY dict_ops (id UInt64, name String, n UInt64)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_ops')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_ops (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_ops VALUES (1), (2);

SELECT 'op equals, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) = 0 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op notEquals, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) != 'zzz' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op less, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) < 100 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op lessOrEquals, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) <= 0 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op greater, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE 'apple' > dictGetString('dict_ops', 'name', id) ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op greaterOrEquals, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) >= 0 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op like, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op notLike, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT LIKE 'a%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op ilike, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) ILIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op notILike, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT ILIKE 'A%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'op match, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE match(dictGetString('dict_ops', 'name', id), '.*') ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;


-- Per-layout no-rewrite examples (predicate `dictGetString... LIKE '%'` always skips)
CREATE TABLE ref_layouts_simple (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_layouts_simple VALUES (1, 'red');
CREATE TABLE ref_layouts_complex (k1 UInt64, k2 String, name String) ENGINE = MergeTree ORDER BY (k1, k2);
INSERT INTO ref_layouts_complex VALUES (1, 'a', 'red');

CREATE DICTIONARY dict_flat (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_layouts_simple')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY dict_hashed (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_layouts_simple')) LAYOUT(HASHED()) LIFETIME(0);
CREATE DICTIONARY dict_hashed_array (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_layouts_simple')) LAYOUT(HASHED_ARRAY()) LIFETIME(0);
CREATE DICTIONARY dict_sparse_hashed (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_layouts_simple')) LAYOUT(SPARSE_HASHED()) LIFETIME(0);
CREATE DICTIONARY dict_complex (k1 UInt64, k2 String, name String) PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'ref_layouts_complex')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY dict_complex_array (k1 UInt64, k2 String, name String) PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'ref_layouts_complex')) LAYOUT(COMPLEX_KEY_HASHED_ARRAY()) LIFETIME(0);
CREATE DICTIONARY dict_complex_sparse (k1 UInt64, k2 String, name String) PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'ref_layouts_complex')) LAYOUT(COMPLEX_KEY_SPARSE_HASHED()) LIFETIME(0);

SELECT 'layout Flat, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_flat', 'name', id) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'layout Hashed, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_hashed', 'name', id) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'layout HashedArray, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_hashed_array', 'name', id) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'layout SparseHashed, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_sparse_hashed', 'name', id) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'layout ComplexKeyHashed, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_complex', 'name', (id, 'a')) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'layout ComplexHashedArray, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_complex_array', 'name', (id, 'a')) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'layout ComplexKeySparseHashed, no rewrite';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_complex_sparse', 'name', (id, 'a')) LIKE '%' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;


SELECT 'rewrite: equals on non-default - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_hashed', 'name', id) = 'red' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: equals on non-default';
SELECT id FROM data_ops WHERE dictGetString('dict_hashed', 'name', id) = 'red' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: equals on non-default, opt off';
SELECT id FROM data_ops WHERE dictGetString('dict_hashed', 'name', id) = 'red' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'rewrite: greater than non-default - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) > 100 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: greater than non-default';
SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) > 100 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: greater than non-default, opt off';
SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) > 100 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'rewrite: != default (numeric) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) != 0 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: != default (numeric)';
SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) != 0 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: != default (numeric), opt off';
SELECT id FROM data_ops WHERE dictGetUInt64('dict_ops', 'n', id) != 0 ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'rewrite: NOT LIKE default (string) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT LIKE '' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: NOT LIKE default (string)';
SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT LIKE '' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: NOT LIKE default (string), opt off';
SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT LIKE '' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'rewrite: NOT ILIKE default (string) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT ILIKE '' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: NOT ILIKE default (string)';
SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT ILIKE '' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: NOT ILIKE default (string), opt off';
SELECT id FROM data_ops WHERE dictGetString('dict_ops', 'name', id) NOT ILIKE '' ORDER BY id
SETTINGS optimize_inverse_dictionary_lookup = 0;


SELECT 'rewrite: plain key - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT count() FROM data_ops WHERE dictGetString('dict_ops', 'name', id) = 'apple'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: plain key';
SELECT count() FROM data_ops WHERE dictGetString('dict_ops', 'name', id) = 'apple'
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: plain key, opt off';
SELECT count() FROM data_ops WHERE dictGetString('dict_ops', 'name', id) = 'apple'
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'rewrite: Nullable(K) key - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT count() FROM data_str_nullable WHERE dictGetString('dict_string', 'name', id) = 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: Nullable(K) key';
SELECT count() FROM data_str_nullable WHERE dictGetString('dict_string', 'name', id) = 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: Nullable(K) key, opt off';
SELECT count() FROM data_str_nullable WHERE dictGetString('dict_string', 'name', id) = 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'rewrite: LC(Nullable(K)) key - plan';
EXPLAIN SYNTAX run_query_tree_passes=1 SELECT count() FROM data_str_lc_nullable WHERE dictGetString('dict_string', 'name', id) = 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'rewrite: LC(Nullable(K)) key';
SELECT count() FROM data_str_lc_nullable WHERE dictGetString('dict_string', 'name', id) = 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'rewrite: LC(Nullable(K)) key, opt off';
SELECT count() FROM data_str_lc_nullable WHERE dictGetString('dict_string', 'name', id) = 'abc'
SETTINGS optimize_inverse_dictionary_lookup = 0;


SELECT 'short-circuited bad-regex match: optimization on, returns 0';
SELECT count() FROM data_ops WHERE id < 0 AND match(dictGetString('dict_ops', 'name', id), '[unclosed')
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'short-circuited bad-regex match: optimization on, returns 0, opt off';
SELECT count() FROM data_ops WHERE id < 0 AND match(dictGetString('dict_ops', 'name', id), '[unclosed')
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'short-circuited bad-regex match, no rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_ops WHERE id < 0 AND match(dictGetString('dict_ops', 'name', id), '[unclosed')
SETTINGS optimize_inverse_dictionary_lookup = 1;


-- max_rows_in_set / max_bytes_in_set with set_overflow_mode = 'break': skip to avoid silent truncation
CREATE TABLE ref_set_limit (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_set_limit VALUES (1, 'hit'), (2, 'hit'), (3, 'miss');
CREATE DICTIONARY dict_set_limit (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_set_limit')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_set_limit (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_set_limit VALUES (1), (2), (3);

SELECT 'max_rows_in_set + break, no rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT groupArray(id) FROM data_set_limit WHERE dictGetString('dict_set_limit', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1, max_rows_in_set = 1, set_overflow_mode = 'break';

SELECT 'max_rows_in_set + break, no rewrite';
SELECT groupArray(id) FROM data_set_limit WHERE dictGetString('dict_set_limit', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1, max_rows_in_set = 1, set_overflow_mode = 'break';
SELECT 'max_rows_in_set + throw, rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT groupArray(id) FROM data_set_limit WHERE dictGetString('dict_set_limit', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1, max_rows_in_set = 1, set_overflow_mode = 'throw';

SELECT 'max_rows_in_set + throw: execution raises SET_SIZE_LIMIT_EXCEEDED';
SELECT groupArray(id) FROM data_set_limit WHERE dictGetString('dict_set_limit', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1, max_rows_in_set = 1, set_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'max_bytes_in_set + break, no rewrite';
SELECT groupArray(id) FROM data_set_limit WHERE dictGetString('dict_set_limit', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1, max_bytes_in_set = 1, set_overflow_mode = 'break';


DROP DICTIONARY dict_int_default_minus1;
DROP DICTIONARY dict_int_nullable_default_minus1;
DROP DICTIONARY dict_string;
DROP DICTIONARY dict_complex_str;
DROP DICTIONARY dict_nullable_attr_stored;
DROP DICTIONARY dict_complex_nullable_dict_key;
DROP DICTIONARY dict_set_limit;
DROP DICTIONARY dict_ops;
DROP DICTIONARY dict_flat;
DROP DICTIONARY dict_hashed;
DROP DICTIONARY dict_hashed_array;
DROP DICTIONARY dict_sparse_hashed;
DROP DICTIONARY dict_complex;
DROP DICTIONARY dict_complex_array;
DROP DICTIONARY dict_complex_sparse;
DROP TABLE ref_int;
DROP TABLE ref_int_nullable;
DROP TABLE ref_str;
DROP TABLE ref_complex_str;
DROP TABLE ref_nullable_attr_stored;
DROP TABLE ref_complex_nullable_dict_key;
DROP TABLE ref_set_limit;
DROP TABLE ref_ops;
DROP TABLE ref_layouts_simple;
DROP TABLE ref_layouts_complex;
DROP TABLE data_int;
DROP TABLE data_str;
DROP TABLE data_str_nullable;
DROP TABLE data_str_lc_nullable;
DROP TABLE data_complex;
DROP TABLE data_complex_lc_nullable;
DROP TABLE data_complex_nullable_dict_key;
DROP TABLE data_outer_nullable_tuple;
DROP TABLE data_set_limit;
DROP TABLE data_ops;
