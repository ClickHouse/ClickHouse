-- Tags: no-fasttest
-- no-fasttest: Parquet format is not available in fasttest builds

-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;

-- Nullable struct with non-nullable elements
DROP TABLE IF EXISTS test_nullable_tuple_basic;
CREATE TABLE test_nullable_tuple_basic (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_basic VALUES ((1, 'a')), (NULL), ((3, 'c'));

-- Parquet Arrow reader
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_basic;
SELECT c0 FROM file(currentDatabase() || '_04065.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_basic;

-- Both struct and element nullable: Nullable(Tuple(Nullable(UInt32), String))
DROP TABLE IF EXISTS test_nullable_tuple_both;
CREATE TABLE test_nullable_tuple_both (c0 Nullable(Tuple(Nullable(UInt32), String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_both VALUES ((1, 'a')), (NULL), ((NULL, 'c')), ((4, 'd'));

-- Parquet Arrow reader both nullable
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_both.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_both;
SELECT c0 FROM file(currentDatabase() || '_04065_both.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_both.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_both;

-- Non-nullable struct with nullable elements
DROP TABLE IF EXISTS test_nullable_tuple_elem;
CREATE TABLE test_nullable_tuple_elem (c0 Tuple(Nullable(UInt32), String)) ENGINE = Memory;
INSERT INTO test_nullable_tuple_elem VALUES ((1, 'a')), ((NULL, 'b'));

-- Parquet Arrow reader nullable elements
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_elem.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_elem;
SELECT c0 FROM file(currentDatabase() || '_04065_elem.parquet', 'Parquet', 'c0 Tuple(Nullable(UInt32), String)') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader nullable elements
SELECT c0 FROM file(currentDatabase() || '_04065_elem.parquet', 'Parquet', 'c0 Tuple(Nullable(UInt32), String)') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_nullable_tuple_elem;

-- Plain non-nullable tuple
DROP TABLE IF EXISTS test_nullable_tuple_plain;
CREATE TABLE test_nullable_tuple_plain (c0 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO test_nullable_tuple_plain VALUES ((1, 'a')), ((2, 'b'));

-- Parquet Arrow reader plain
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_plain.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_plain;
SELECT c0 FROM file(currentDatabase() || '_04065_plain.parquet', 'Parquet', 'c0 Tuple(UInt32, String)') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader plain
SELECT c0 FROM file(currentDatabase() || '_04065_plain.parquet', 'Parquet', 'c0 Tuple(UInt32, String)') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_nullable_tuple_plain;

-- Named tuple
DROP TABLE IF EXISTS test_nullable_tuple_named;
CREATE TABLE test_nullable_tuple_named (c0 Nullable(Tuple(a UInt32, b String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_named VALUES ((1, 'x')), (NULL), ((3, 'z'));

-- Parquet Arrow reader named
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_named.parquet', 'Parquet', 'c0 Nullable(Tuple(a UInt32, b String))') SELECT c0 FROM test_nullable_tuple_named;
SELECT c0 FROM file(currentDatabase() || '_04065_named.parquet', 'Parquet', 'c0 Nullable(Tuple(a UInt32, b String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader named (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_named.parquet', 'Parquet', 'c0 Nullable(Tuple(a UInt32, b String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_named;

-- All-NULL column
DROP TABLE IF EXISTS test_nullable_tuple_allnull;
CREATE TABLE test_nullable_tuple_allnull (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_allnull VALUES (NULL), (NULL), (NULL);

-- Parquet Arrow reader all null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_allnull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_allnull;
SELECT c0 FROM file(currentDatabase() || '_04065_allnull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader all null (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_allnull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_allnull;

-- No-NULL column (nullable type, zero actual NULLs)
DROP TABLE IF EXISTS test_nullable_tuple_nonull;
CREATE TABLE test_nullable_tuple_nonull (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_nonull VALUES ((1, 'a')), ((2, 'b')), ((3, 'c'));

-- Parquet Arrow reader no null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_nonull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_nonull;
SELECT c0 FROM file(currentDatabase() || '_04065_nonull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader no null (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_nonull.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_nonull;

-- Single-element tuple
DROP TABLE IF EXISTS test_nullable_tuple_single;
CREATE TABLE test_nullable_tuple_single (c0 Nullable(Tuple(UInt32))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_single VALUES ((1,)), (NULL), ((3,));

-- Parquet Arrow reader single
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_single.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32))') SELECT c0 FROM test_nullable_tuple_single;
SELECT c0 FROM file(currentDatabase() || '_04065_single.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader single (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_single.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_single;

-- Deeply nested: nullable tuple inside nullable tuple
DROP TABLE IF EXISTS test_nullable_tuple_deep;
CREATE TABLE test_nullable_tuple_deep (c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_deep VALUES (((1, 'a'), 10)), (NULL), ((NULL, 20)), (((4, 'd'), 40));

-- Parquet Arrow reader deep nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_deep.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_deep;
SELECT c0 FROM file(currentDatabase() || '_04065_deep.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader deep nested (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_deep.parquet', 'Parquet', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_deep;

-- Nullable tuple with Array element
DROP TABLE IF EXISTS test_nullable_tuple_arr;
CREATE TABLE test_nullable_tuple_arr (c0 Nullable(Tuple(Array(UInt32), String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_arr VALUES (([1, 2], 'a')), (NULL), (([3], 'c'));

-- Parquet Arrow reader array elem
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_arr.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_arr;
SELECT c0 FROM file(currentDatabase() || '_04065_arr.parquet', 'Parquet', 'c0 Nullable(Tuple(Array(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader array elem (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_arr.parquet', 'Parquet', 'c0 Nullable(Tuple(Array(UInt32), String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_arr;

-- Multiple nullable tuple columns
DROP TABLE IF EXISTS test_nullable_tuple_multi;
CREATE TABLE test_nullable_tuple_multi (c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_multi VALUES ((1, 'a'), (1.5)), (NULL, (2.5)), ((3, 'c'), NULL);

-- Parquet Arrow reader multi col
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_multi.parquet', 'Parquet') SELECT c0, c1 FROM test_nullable_tuple_multi;
SELECT c0, c1 FROM file(currentDatabase() || '_04065_multi.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader multi col (not yet supported)
SELECT c0, c1 FROM file(currentDatabase() || '_04065_multi.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_multi;

-- Schema inference without type hint (works for both readers, but V3 loses struct-level NULL)
DROP TABLE IF EXISTS test_nullable_tuple_infer;
CREATE TABLE test_nullable_tuple_infer (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_infer VALUES ((1, 'a')), (NULL), ((3, 'c'));

-- Parquet Arrow reader infer
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_infer.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_infer;
SELECT c0 FROM file(currentDatabase() || '_04065_infer.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

DROP TABLE test_nullable_tuple_infer;

-- Type hint mismatch: file has Nullable(Tuple(...)), read as Tuple(...) (strip nullable, NULLs become defaults)
DROP TABLE IF EXISTS test_nullable_tuple_mismatch1;
CREATE TABLE test_nullable_tuple_mismatch1 (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_mismatch1 VALUES ((1, 'a')), (NULL), ((3, 'c'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_mismatch1.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_mismatch1;

-- Parquet Arrow reader: read nullable file as non-nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_mismatch1.parquet', 'Parquet', 'c0 Tuple(UInt32, String)') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

DROP TABLE test_nullable_tuple_mismatch1;

-- Type hint mismatch: file has Tuple(...), read as Nullable(Tuple(...)) (add nullable wrapper)
DROP TABLE IF EXISTS test_nullable_tuple_mismatch2;
CREATE TABLE test_nullable_tuple_mismatch2 (c0 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO test_nullable_tuple_mismatch2 VALUES ((1, 'a')), ((2, 'b'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_mismatch2.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_mismatch2;

-- Parquet Arrow reader: read non-nullable file as nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_mismatch2.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader: read non-nullable file as nullable (not yet supported)
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_mismatch2.parquet', 'Parquet', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_mismatch2;

-- Schema inference: inferred type with toTypeName
DROP TABLE IF EXISTS test_nullable_tuple_describe;
CREATE TABLE test_nullable_tuple_describe (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_describe VALUES ((1, 'a')), (NULL), ((3, 'c'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_describe.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_describe;

-- Parquet Arrow reader: inferred type
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_describe.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader: inferred type (struct-level NULL not supported, becomes (NULL,NULL))
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_describe.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_nullable_tuple_describe;

-- Array(Nullable(Tuple)) flattened via import_nested: struct-level NULLs should propagate to elements
DROP TABLE IF EXISTS test_nullable_tuple_import_nested;
CREATE TABLE test_nullable_tuple_import_nested (c0 Array(Nullable(Tuple(a UInt32, b String)))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_import_nested VALUES ([(1, 'a'), NULL, (3, 'c')]);

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_import_nested.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_import_nested;

-- Parquet Arrow reader import_nested
SELECT * FROM file(currentDatabase() || '_04065_import_nested.parquet', 'Parquet', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Nullable(String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0, input_format_parquet_import_nested = 1;

-- Parquet V3 native reader import_nested
-- This works because V3 reader sees the already-flattened column names (c0.a, c0.b), not the Nullable(Tuple(...))
SELECT * FROM file(currentDatabase() || '_04065_import_nested.parquet', 'Parquet', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Nullable(String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_import_nested = 1;

DROP TABLE test_nullable_tuple_import_nested;

-- Array(Nullable(Tuple)) without named elements: round-trip as a single column, no flattening
DROP TABLE IF EXISTS test_nullable_tuple_arr_unnamed;
CREATE TABLE test_nullable_tuple_arr_unnamed (c0 Array(Nullable(Tuple(UInt32, String)))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_arr_unnamed VALUES ([(1, 'a'), NULL, (3, 'c')]);

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_arr_unnamed.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_arr_unnamed;

-- Parquet Arrow reader unnamed
SELECT c0 FROM file(currentDatabase() || '_04065_arr_unnamed.parquet', 'Parquet', 'c0 Array(Nullable(Tuple(UInt32, String)))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader unnamed (not yet supported)
SELECT c0 FROM file(currentDatabase() || '_04065_arr_unnamed.parquet', 'Parquet', 'c0 Array(Nullable(Tuple(UInt32, String)))') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- { serverError TYPE_MISMATCH }

DROP TABLE test_nullable_tuple_arr_unnamed;

-- Array(Nullable(Tuple)) with Array element inside: import_nested flattens, Array defaults to [] at null positions
DROP TABLE IF EXISTS test_nullable_tuple_arr_nested_elem;
CREATE TABLE test_nullable_tuple_arr_nested_elem (c0 Array(Nullable(Tuple(a UInt32, b Array(UInt32))))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_arr_nested_elem VALUES ([(1, [10, 20]), NULL, (3, [30])]);

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_arr_nested_elem.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_arr_nested_elem;

-- Parquet Arrow reader import_nested: scalar becomes Nullable, Array defaults to [] at null struct positions
SELECT * FROM file(currentDatabase() || '_04065_arr_nested_elem.parquet', 'Parquet', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Array(UInt32))') SETTINGS input_format_parquet_use_native_reader_v3 = 0, input_format_parquet_import_nested = 1;

-- Parquet V3 native reader import_nested
SELECT * FROM file(currentDatabase() || '_04065_arr_nested_elem.parquet', 'Parquet', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Array(UInt32))') SETTINGS input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_import_nested = 1;

DROP TABLE test_nullable_tuple_arr_nested_elem;

-- LowCardinality(Nullable(String)) hint with no physical nulls in the file: the reader must still wrap the column as nullable
DROP TABLE IF EXISTS test_nullable_tuple_lc_string;
CREATE TABLE test_nullable_tuple_lc_string (c0 String) ENGINE = Memory;
INSERT INTO test_nullable_tuple_lc_string VALUES ('hello'), ('world');

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04065_lc_str.parquet', 'Parquet') SELECT c0 FROM test_nullable_tuple_lc_string;

-- Parquet Arrow reader: no physical nulls, LowCardinality(Nullable(String)) hint
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_lc_str.parquet', 'Parquet', 'c0 LowCardinality(Nullable(String))') SETTINGS input_format_parquet_use_native_reader_v3 = 0;

-- Parquet V3 native reader: no physical nulls, LowCardinality(Nullable(String)) hint
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04065_lc_str.parquet', 'Parquet', 'c0 LowCardinality(Nullable(String))') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

DROP TABLE test_nullable_tuple_lc_string;
