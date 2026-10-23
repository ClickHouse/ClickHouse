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

DROP TABLE IF EXISTS t_tuple_key;
CREATE TABLE t_tuple_key (t Tuple(String)) ENGINE = MergeTree ORDER BY tuple(t) SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO t_tuple_key VALUES (('z')), (('a')), (('3'));

DROP TABLE IF EXISTS t_tuple_partition;
CREATE TABLE t_tuple_partition (v Tuple(String)) ENGINE = MergeTree ORDER BY tuple(v) PARTITION BY v SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO t_tuple_partition VALUES (('z')), (('a')), (('3'));

DROP TABLE IF EXISTS t_string_derived_key;
CREATE TABLE t_string_derived_key (s String) ENGINE = MergeTree ORDER BY (toString(s), s) SETTINGS index_granularity = 1;
INSERT INTO t_string_derived_key VALUES ('z'), ('a'), ('m');

DROP TABLE IF EXISTS t_enum_number_name;
CREATE TABLE t_enum_number_name (e Enum8('z' = 1, 'a' = 2, '1' = 3)) ENGINE = MergeTree ORDER BY e PARTITION BY e SETTINGS index_granularity = 1;
INSERT INTO t_enum_number_name VALUES ('z'), ('a'), ('1');

DROP TABLE IF EXISTS t_enum_number_chain;
CREATE TABLE t_enum_number_chain (x Enum8('z' = 1, 'a' = 2, '1' = 3), e Enum8('z' = 1, 'a' = 2, '1' = 3)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO t_enum_number_chain VALUES ('z', 'a');

DROP TABLE IF EXISTS t_nested_tuple_partition;
CREATE TABLE t_nested_tuple_partition (v Tuple(Tuple(String))) ENGINE = MergeTree ORDER BY tuple(v) PARTITION BY v SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO t_nested_tuple_partition VALUES ((('z'))), ((('a'))), ((('3')));

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
SELECT count() FROM t_enum_cmp WHERE e < 'm' SETTINGS force_primary_key = 1, optimize_use_implicit_projections = 0;
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
SELECT if(s = 'q', NULL, tuple(s)) < tuple(CAST(CAST('a', 'Enum8(''z'' = 1, ''a'' = 2)') AS Dynamic)) FROM values('s String', 'q', 'z') SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT if(s = 'q', NULL, tuple(s)) < tuple(CAST(CAST('a', 'Enum8(''z'' = 1, ''a'' = 2)') AS Variant(Array(UInt8), Enum8('z' = 1, 'a' = 2)))) FROM values('s String', 'q', 'z') SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT tuple(nullIf(s, 'q'), 0) < tuple(e, 1) FROM values('e Enum8(''z'' = 1, ''a'' = 2), s String', ('z', 'q'), ('z', 'a')) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT tuple(s, 0) < tuple(CAST('a', 'Enum8(''z'' = 1, ''a'' = 2)'), 1) FROM values('s LowCardinality(String)', 'z', 'a');

-- a condition can skip the rows with an unknown name
SELECT if(s = 'q', 0, e < s) FROM values('e Enum8(''z'' = 1, ''a'' = 2), s String', ('z', 'q'), ('z', 'a')) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(s = 'q', 0, s > e) FROM values('e Enum8(''z'' = 1, ''a'' = 2), s String', ('z', 'q'), ('z', 'a')) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(s = 'q', 0, tuple(s) < tuple(CAST(CAST('a', 'Enum8(''z'' = 1, ''a'' = 2)') AS Dynamic))) FROM values('s String', 'q', 'z') SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(s = 'q', 0, tuple(s) < tuple(CAST(CAST('a', 'Enum8(''z'' = 1, ''a'' = 2)') AS Variant(Array(UInt8), Enum8('z' = 1, 'a' = 2))))) FROM values('s String', 'q', 'z') SETTINGS short_circuit_function_evaluation = 'enable';

-- the redundant comparison folding keeps the Enum order in a tuple element
SELECT count() FROM values('s String, k UInt8', ('z', 1)) WHERE (s, k) < (CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)'), 0) AND (s, k) > ('z', 0) SETTINGS optimize_and_compare_chain = 0;
SELECT arraySort(groupArray(t.1)) FROM t_tuple_key WHERE t < tuple(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)')) SETTINGS optimize_use_implicit_projections = 0;

-- a constant string converts to the Enum as a column does, regardless of the format settings
SELECT '1' < CAST('a', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)'), materialize('1') < CAST('a', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)') SETTINGS input_format_tsv_enum_as_number = 1;
SELECT countIf(materialize('1') < CAST('a', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)')) FROM numbers(2) SETTINGS input_format_tsv_enum_as_number = 1;
SELECT count() FROM numbers(2) WHERE materialize('1') < CAST('a', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)') SETTINGS input_format_tsv_enum_as_number = 1;
SELECT count() FROM t_enum_number_name WHERE e < '1' SETTINGS input_format_tsv_enum_as_number = 1;
SELECT arraySort(groupArray(e)) FROM t_enum_number_name WHERE e < '1' SETTINGS input_format_tsv_enum_as_number = 1, optimize_use_implicit_projections = 0;
SELECT tuple('1', 0) < tuple(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)'), 1), materialize(tuple('1', 0)) < tuple(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)'), 1) SETTINGS input_format_tsv_enum_as_number = 1;
SELECT tuple(tuple('1'), 0) < tuple(tuple(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)')), 1), materialize(tuple(tuple('1'), 0)) < tuple(tuple(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)')), 1) SETTINGS input_format_tsv_enum_as_number = 1;
SELECT tuple('1', 0) < tuple(CAST(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)') AS Dynamic), 1), materialize(tuple('1', 0)) < tuple(CAST(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)') AS Dynamic), 1) SETTINGS input_format_tsv_enum_as_number = 1;
SELECT count() FROM numbers(2) WHERE materialize(tuple('1', 0)) < tuple(CAST('1', 'Enum8(''z'' = 1, ''a'' = 2, ''1'' = 3)'), 1) SETTINGS input_format_tsv_enum_as_number = 1;
SELECT count() FROM t_enum_number_chain WHERE x < e AND e < '1' SETTINGS input_format_tsv_enum_as_number = 1, optimize_use_implicit_projections = 0;

-- the prefix equality of a tuple ordering reads a number as the Enum value, as the ordering does
SELECT countIf(materialize(tuple('1', 0)) < tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1)) FROM numbers(2);
SELECT count() FROM numbers(2) WHERE materialize(tuple('1', 0)) < tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1);
SELECT tuple('1', 0) < tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), tuple('1', 0) <= tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), tuple('1', 0) > tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), tuple('1', 0) >= tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1) > tuple('1', 0);
SELECT materialize(tuple('1', 0)) < tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), materialize(tuple('1', 0)) <= tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), materialize(tuple('1', 0)) > tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), materialize(tuple('1', 0)) >= tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1), tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1) > materialize(tuple('1', 0));
SELECT tuple(tuple('1'), 0) < tuple(tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)')), 1), materialize(tuple(tuple('1'), 0)) < tuple(tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)')), 1);
SELECT tuple('1', 0) < tuple(CAST(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)') AS Dynamic), 1), materialize(tuple('1', 0)) < tuple(CAST(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)') AS Dynamic), 1);

-- the key analysis keeps the Enum order of a Dynamic or Variant tuple element
SELECT arraySort(groupArray(v.1)) FROM t_tuple_partition WHERE v < tuple(CAST(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)') AS Dynamic)) SETTINGS optimize_use_implicit_projections = 0;
SELECT arraySort(groupArray(v.1)) FROM t_tuple_partition WHERE v < tuple(CAST(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)') AS Variant(Enum8('z' = 1, 'a' = 2, '3' = 3), Array(UInt8)))) SETTINGS optimize_use_implicit_projections = 0;
SELECT arraySort(groupArray(v.1)) FROM t_tuple_partition WHERE v < CAST(tuple(CAST(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)') AS Dynamic)) AS Dynamic) SETTINGS optimize_use_implicit_projections = 0;
SELECT arraySort(groupArray(t.1)) FROM t_tuple_key WHERE t < tuple(CAST(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)') AS Dynamic)) SETTINGS optimize_use_implicit_projections = 0;

-- the key analysis keeps the Enum order of a Dynamic tuple element that holds a tuple
SELECT arraySort(groupArray(v.1.1)) FROM t_nested_tuple_partition WHERE v < tuple(CAST(tuple(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)')) AS Dynamic)) SETTINGS optimize_use_implicit_projections = 0;
SELECT arraySort(groupArray(v.1.1)) FROM t_nested_tuple_partition WHERE v < tuple(CAST(tuple(CAST('3', 'Enum8(''z'' = 1, ''a'' = 2, ''3'' = 3)')) AS Dynamic)) SETTINGS optimize_use_implicit_projections = 0, use_partition_pruning = 0;

-- equality keeps the primary key of a string column
SELECT s FROM t_string_key WHERE isNotDistinctFrom(s, CAST('a', 'Enum8(''z'' = 1, ''a'' = 2, ''m'' = 3)')) SETTINGS force_primary_key = 1, optimize_use_implicit_projections = 0;

SELECT e < 'q' FROM t_enum_cmp; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT e < materialize('q') FROM t_enum_cmp; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE t_enum_cmp;
DROP TABLE t_enum_chain;
DROP TABLE t_enum_name_key;
DROP TABLE t_string_key;
DROP TABLE t_string_derived_key;
DROP TABLE t_tuple_key;
DROP TABLE t_tuple_partition;
DROP TABLE t_enum_number_name;
DROP TABLE t_enum_number_chain;
DROP TABLE t_nested_tuple_partition;
