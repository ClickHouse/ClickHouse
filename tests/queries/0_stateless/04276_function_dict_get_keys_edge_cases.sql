-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

SET enable_analyzer = 1;

DROP DICTIONARY IF EXISTS dict_float_nan;
DROP DICTIONARY IF EXISTS dict_date_attr;
DROP DICTIONARY IF EXISTS dict_decimal_attr;
DROP DICTIONARY IF EXISTS dict_wide_key;
DROP DICTIONARY IF EXISTS dict_nullable_predicate;
DROP TABLE IF EXISTS ref_float_nan;
DROP TABLE IF EXISTS ref_date_attr;
DROP TABLE IF EXISTS ref_decimal_attr;
DROP TABLE IF EXISTS ref_wide_key;
DROP TABLE IF EXISTS ref_nullable_predicate;
DROP TABLE IF EXISTS data_float_nan;
DROP TABLE IF EXISTS data_date_attr;
DROP TABLE IF EXISTS data_decimal_attr;
DROP TABLE IF EXISTS data_narrow_key;
DROP TABLE IF EXISTS data_nullable_predicate;


-- Float NaN: `equals(NaN, NaN)` is false, so the inverse-lookup rewrite must produce the
-- same empty result as the unoptimized query.
CREATE TABLE ref_float_nan (id UInt64, f Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_float_nan VALUES (1, nan), (2, 1.0);
CREATE DICTIONARY dict_float_nan (id UInt64, f Float64)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_float_nan')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_float_nan (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_float_nan VALUES (1), (2), (3);

SELECT 'Float NaN, dictGetKeys constant';
SELECT dictGetKeys('dict_float_nan', 'f', nan);

SELECT 'Float NaN, rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT groupArray(id) FROM data_float_nan WHERE dictGetFloat64('dict_float_nan', 'f', id) = nan
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Float NaN, rewrite';
SELECT groupArray(id) FROM data_float_nan WHERE dictGetFloat64('dict_float_nan', 'f', id) = nan
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Float NaN, no rewrite';
SELECT groupArray(id) FROM data_float_nan WHERE dictGetFloat64('dict_float_nan', 'f', id) = nan
SETTINGS optimize_inverse_dictionary_lookup = 0;


-- Date attr compared to DateTime constant: SQL `=` promotes Date to DateTime(midnight), no
-- match at noon. Rewrite must not cast the DateTime to Date inside `dictGetKeys` and find
-- spurious matches.
CREATE TABLE ref_date_attr (id UInt64, d Date) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_date_attr VALUES (1, '2025-01-01'), (2, '2025-01-02');
CREATE DICTIONARY dict_date_attr (id UInt64, d Date)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_date_attr')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_date_attr (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_date_attr VALUES (1), (2), (3);

SELECT 'Date attr vs DateTime constant, rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT groupArray(id) FROM data_date_attr WHERE dictGet('dict_date_attr', 'd', id) = toDateTime('2025-01-01 12:00:00')
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Date attr vs DateTime constant, rewrite';
SELECT groupArray(id) FROM data_date_attr WHERE dictGet('dict_date_attr', 'd', id) = toDateTime('2025-01-01 12:00:00')
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Date attr vs DateTime constant, no rewrite';
SELECT groupArray(id) FROM data_date_attr WHERE dictGet('dict_date_attr', 'd', id) = toDateTime('2025-01-01 12:00:00')
SETTINGS optimize_inverse_dictionary_lookup = 0;


-- Decimal attr compared to Float constant: same root cause as the Date / DateTime case.
CREATE TABLE ref_decimal_attr (id UInt64, p Decimal64(2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_decimal_attr VALUES (1, '1.23');
CREATE DICTIONARY dict_decimal_attr (id UInt64, p Decimal64(2))
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_decimal_attr')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_decimal_attr (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_decimal_attr VALUES (1), (2);

SELECT 'Decimal attr vs Float constant, rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT groupArray(id) FROM data_decimal_attr WHERE dictGet('dict_decimal_attr', 'p', id) = toFloat64(1.234)
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Decimal attr vs Float constant, rewrite';
SELECT groupArray(id) FROM data_decimal_attr WHERE dictGet('dict_decimal_attr', 'p', id) = toFloat64(1.234)
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Decimal attr vs Float constant, no rewrite';
SELECT groupArray(id) FROM data_decimal_attr WHERE dictGet('dict_decimal_attr', 'p', id) = toFloat64(1.234)
SETTINGS optimize_inverse_dictionary_lookup = 0;


-- Dictionary key wider than the query expression: the single-match folded form must use
-- the dictionary's key type for the constant (not the query column's narrower type), so
-- planning doesn't throw `ARGUMENT_OUT_OF_BOUND` on a key that exceeds the column type.
CREATE TABLE ref_wide_key (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_wide_key VALUES (300, 'hit'), (1, 'one');
CREATE DICTIONARY dict_wide_key (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_wide_key')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_narrow_key (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO data_narrow_key VALUES (1), (44), (255);

SELECT 'Wide dict key vs narrow column, rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT groupArray(id) FROM data_narrow_key WHERE dictGetString('dict_wide_key', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Wide dict key vs narrow column, rewrite';
SELECT groupArray(id) FROM data_narrow_key WHERE dictGetString('dict_wide_key', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Wide dict key vs narrow column, no rewrite';
SELECT groupArray(id) FROM data_narrow_key WHERE dictGetString('dict_wide_key', 'name', id) = 'hit'
SETTINGS optimize_inverse_dictionary_lookup = 0;


-- Empty `dictGetKeys` result with a Nullable predicate: replacing the predicate with a
-- non-null `0` would lose the NULL produced by `dictGet(NULL key) = const`. Visible via
-- `isNull(predicate)`.
CREATE TABLE ref_nullable_predicate (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_nullable_predicate VALUES (1, 'abc');
CREATE DICTIONARY dict_nullable_predicate (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'ref_nullable_predicate')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE data_nullable_predicate (id Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO data_nullable_predicate VALUES (1), (2), (NULL);

SELECT 'Empty rewrite, Nullable predicate, rewrite - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data_nullable_predicate WHERE isNull(dictGet('dict_nullable_predicate', 'name', id) = 'zzz')
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Empty rewrite, Nullable predicate, rewrite';
SELECT count() FROM data_nullable_predicate WHERE isNull(dictGet('dict_nullable_predicate', 'name', id) = 'zzz')
SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'Empty rewrite, Nullable predicate, no rewrite';
SELECT count() FROM data_nullable_predicate WHERE isNull(dictGet('dict_nullable_predicate', 'name', id) = 'zzz')
SETTINGS optimize_inverse_dictionary_lookup = 0;


DROP DICTIONARY dict_float_nan;
DROP DICTIONARY dict_date_attr;
DROP DICTIONARY dict_decimal_attr;
DROP DICTIONARY dict_wide_key;
DROP DICTIONARY dict_nullable_predicate;
DROP TABLE ref_float_nan;
DROP TABLE ref_date_attr;
DROP TABLE ref_decimal_attr;
DROP TABLE ref_wide_key;
DROP TABLE ref_nullable_predicate;
DROP TABLE data_float_nan;
DROP TABLE data_date_attr;
DROP TABLE data_decimal_attr;
DROP TABLE data_narrow_key;
DROP TABLE data_nullable_predicate;
