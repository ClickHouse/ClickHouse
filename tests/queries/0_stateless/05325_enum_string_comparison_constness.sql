-- comparing Enum with String gives the same result for constant and non-constant strings

DROP TABLE IF EXISTS t_enum_cmp;
CREATE TABLE t_enum_cmp (e Enum8('z' = 1, 'a' = 2, 'm' = 3), s String, fs FixedString(1))
ENGINE = MergeTree ORDER BY e PARTITION BY e SETTINGS index_granularity = 1;
INSERT INTO t_enum_cmp VALUES ('z', 'a', 'a'), ('a', 'm', 'm'), ('m', 'z', 'z');

DROP TABLE IF EXISTS t_enum_chain;
CREATE TABLE t_enum_chain (x Enum8('z' = 1, 'a' = 2, 'm' = 3, 'b' = 4), e Enum8('z' = 1, 'a' = 2, 'm' = 3, 'b' = 4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_chain VALUES ('m', 'b');

DROP TABLE IF EXISTS t_string_key;
CREATE TABLE t_string_key (s String) ENGINE = MergeTree ORDER BY s PARTITION BY s SETTINGS index_granularity = 1;
INSERT INTO t_string_key VALUES ('z'), ('a'), ('m');

DROP TABLE IF EXISTS t_enum_name_key;
CREATE TABLE t_enum_name_key (e Enum8('z' = 1, 'a' = 2, 'm' = 3)) ENGINE = MergeTree ORDER BY toString(e) SETTINGS index_granularity = 1;
INSERT INTO t_enum_name_key VALUES ('z'), ('a'), ('m');

DROP TABLE IF EXISTS t_string_derived_key;
CREATE TABLE t_string_derived_key (s String) ENGINE = MergeTree ORDER BY (toString(s), s) SETTINGS index_granularity = 1;
INSERT INTO t_string_derived_key VALUES ('z'), ('a'), ('m');

SET optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1;

SELECT 'a' < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), materialize('a') < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)');
SELECT x < e FROM (SELECT 'a' AS x, CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)') AS e);
SELECT e, s, e < s, e > s, e <= s, e >= s, e = s, e != s FROM t_enum_cmp ORDER BY e;
SELECT e, fs < e, toNullable(s) < e, toLowCardinality(s) < e, tuple(s) < tuple(e) FROM t_enum_cmp ORDER BY e;
SELECT e, e < 'm', e < materialize('m'), e < toFixedString('m', 1), tuple(e) < tuple('m') FROM t_enum_cmp ORDER BY e;

-- toString compares the names
SELECT e, toString(e) < 'm', toString(e) < materialize('m'), toString(e) < s, toString(e) < 'q' FROM t_enum_cmp ORDER BY e;

-- the indexes, the implicit projections and the analyzer use the order of the comparison
SELECT count() FROM t_enum_cmp WHERE e > 'm';
SELECT count() FROM t_enum_cmp WHERE e < 'm';
SELECT count() FROM t_enum_cmp WHERE e < materialize('m');
SELECT count() FROM t_enum_cmp WHERE e < materialize('m') SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_enum_cmp WHERE e = 'm';
SELECT count() FROM t_enum_cmp WHERE e > 'm' AND e = 'z';
SELECT count() FROM t_enum_cmp WHERE e >= 'm' AND e <= 'z';
SELECT count() FROM t_enum_cmp WHERE toString(e) > 'm';
SELECT count() FROM t_enum_chain WHERE x < e AND e < 'm';
SELECT count() FROM t_enum_name_key WHERE e < 'm';
SELECT count() FROM t_enum_name_key WHERE e > 'a';
SELECT count() FROM t_string_key WHERE s < CAST('m', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)');
SELECT count() FROM t_string_key WHERE s < CAST('m', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)') AND s = 'z';
SELECT count() FROM t_string_derived_key WHERE s < CAST('m', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)');
SELECT count() FROM t_string_derived_key WHERE s < toNullable(CAST('m', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)'));
SELECT count() FROM t_string_derived_key WHERE s < CAST(CAST('m', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)') AS Dynamic);
SELECT count() FROM t_string_derived_key WHERE s < CAST(CAST('m', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)') AS Variant(Array(UInt8), Enum8('z' = 1, 'a' = 2, 'm' = 3)));

-- the unused default of a LowCardinality dictionary and the values behind a NULL are not converted to the Enum
SELECT s < CAST('a', 'Enum8(''z'' = 1, ''a'' = 2)') FROM values('s LowCardinality(String)', 'z', 'a');
SELECT e < s FROM values('e Enum8(''z'' = 1, ''a'' = 2), s Nullable(String)', ('z', NULL), ('z', 'a')) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT e < nullIf(s, 'q') FROM values('e Enum8(''z'' = 1, ''a'' = 2), s String', ('z', 'q'), ('z', 'a')) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT if(s = 'q', NULL, tuple(s)) < tuple(e) FROM values('e Enum8(''z'' = 1, ''a'' = 2), s String', ('z', 'q'), ('z', 'a')) SETTINGS short_circuit_function_evaluation_for_nulls = 0;

-- the redundant comparison folding keeps the Enum order in a tuple element
SELECT count() FROM values('s String, k UInt8', ('z', 1)) WHERE (s, k) < (CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)'), 0) AND (s, k) > ('z', 0) SETTINGS optimize_and_compare_chain = 0;

SELECT e < 'q' FROM t_enum_cmp; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT e < materialize('q') FROM t_enum_cmp; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE t_enum_cmp;
DROP TABLE t_enum_chain;
DROP TABLE t_enum_name_key;
DROP TABLE t_string_key;
DROP TABLE t_string_derived_key;
