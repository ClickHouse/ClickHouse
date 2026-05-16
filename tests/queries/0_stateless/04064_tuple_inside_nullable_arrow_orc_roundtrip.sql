-- Tags: no-fasttest
-- no-fasttest: Arrow and ORC formats are not available in fasttest builds

-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;

-- Nullable struct with non-nullable elements
DROP TABLE IF EXISTS test_nullable_tuple_basic;
CREATE TABLE test_nullable_tuple_basic (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_basic VALUES ((1, 'a')), (NULL), ((3, 'c'));

-- Arrow
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_basic;
SELECT c0 FROM file(currentDatabase() || '_04064.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))');

-- ArrowStream
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_basic;
SELECT c0 FROM file(currentDatabase() || '_04064.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_basic;
SELECT c0 FROM file(currentDatabase() || '_04064.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC legacy (Arrow-based) reader
SELECT c0 FROM file(currentDatabase() || '_04064.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_basic;

-- Nullable empty tuple
DROP TABLE IF EXISTS test_nullable_tuple_empty;
CREATE TABLE test_nullable_tuple_empty (c0 Nullable(Tuple())) ENGINE = Memory;
INSERT INTO test_nullable_tuple_empty VALUES (()), (NULL), (());

-- Arrow empty
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_empty.arrow', 'Arrow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_tuple_empty;
SELECT c0 FROM file(currentDatabase() || '_04064_empty.arrow', 'Arrow', 'c0 Nullable(Tuple())');

-- ArrowStream empty
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_empty.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_tuple_empty;
SELECT c0 FROM file(currentDatabase() || '_04064_empty.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple())');

-- ORC empty
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_empty.orc', 'ORC', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_tuple_empty;
SELECT c0 FROM file(currentDatabase() || '_04064_empty.orc', 'ORC', 'c0 Nullable(Tuple())');

-- ORC legacy empty
SELECT c0 FROM file(currentDatabase() || '_04064_empty.orc', 'ORC', 'c0 Nullable(Tuple())') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_empty;

-- Both struct and element nullable: Nullable(Tuple(Nullable(UInt32), String))
DROP TABLE IF EXISTS test_nullable_tuple_both;
CREATE TABLE test_nullable_tuple_both (c0 Nullable(Tuple(Nullable(UInt32), String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_both VALUES ((1, 'a')), (NULL), ((NULL, 'c')), ((4, 'd'));

-- Arrow both nullable
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_both.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_both;
SELECT c0 FROM file(currentDatabase() || '_04064_both.arrow', 'Arrow', 'c0 Nullable(Tuple(Nullable(UInt32), String))');

-- ArrowStream both nullable
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_both.arrowstream', 'ArrowStream') SELECT c0 FROM test_nullable_tuple_both;
SELECT c0 FROM file(currentDatabase() || '_04064_both.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple(Nullable(UInt32), String))');

-- ORC both nullable
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_both.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_both;
SELECT c0 FROM file(currentDatabase() || '_04064_both.orc', 'ORC', 'c0 Nullable(Tuple(Nullable(UInt32), String))');

-- ORC legacy both nullable
SELECT c0 FROM file(currentDatabase() || '_04064_both.orc', 'ORC', 'c0 Nullable(Tuple(Nullable(UInt32), String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_both;

-- Non-nullable struct with nullable elements (should be unchanged)
DROP TABLE IF EXISTS test_nullable_tuple_elem;
CREATE TABLE test_nullable_tuple_elem (c0 Tuple(Nullable(UInt32), String)) ENGINE = Memory;
INSERT INTO test_nullable_tuple_elem VALUES ((1, 'a')), ((NULL, 'b'));

-- Arrow nullable elements
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_elem.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_elem;
SELECT c0 FROM file(currentDatabase() || '_04064_elem.arrow', 'Arrow', 'c0 Tuple(Nullable(UInt32), String)');

-- ORC nullable elements
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_elem.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_elem;
SELECT c0 FROM file(currentDatabase() || '_04064_elem.orc', 'ORC', 'c0 Tuple(Nullable(UInt32), String)');

-- ORC legacy nullable elements
SELECT c0 FROM file(currentDatabase() || '_04064_elem.orc', 'ORC', 'c0 Tuple(Nullable(UInt32), String)') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_elem;

-- Plain non-nullable tuple (baseline, should be unchanged)
DROP TABLE IF EXISTS test_nullable_tuple_plain;
CREATE TABLE test_nullable_tuple_plain (c0 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO test_nullable_tuple_plain VALUES ((1, 'a')), ((2, 'b'));

-- Arrow plain
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_plain.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_plain;
SELECT c0 FROM file(currentDatabase() || '_04064_plain.arrow', 'Arrow', 'c0 Tuple(UInt32, String)');

-- ORC plain
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_plain.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_plain;
SELECT c0 FROM file(currentDatabase() || '_04064_plain.orc', 'ORC', 'c0 Tuple(UInt32, String)');

-- ORC legacy plain
SELECT c0 FROM file(currentDatabase() || '_04064_plain.orc', 'ORC', 'c0 Tuple(UInt32, String)') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_plain;

-- Nested tuple inside nullable struct
DROP TABLE IF EXISTS test_nullable_tuple_nested;
CREATE TABLE test_nullable_tuple_nested (c0 Nullable(Tuple(Tuple(UInt32, String), UInt64))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_nested VALUES (((1, 'a'), 10)), (NULL), (((3, 'c'), 30));

-- Arrow nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_nested.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_nested;
SELECT c0 FROM file(currentDatabase() || '_04064_nested.arrow', 'Arrow', 'c0 Nullable(Tuple(Tuple(UInt32, String), UInt64))');

-- ORC nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_nested.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_nested;
SELECT c0 FROM file(currentDatabase() || '_04064_nested.orc', 'ORC', 'c0 Nullable(Tuple(Tuple(UInt32, String), UInt64))');

-- ORC legacy nested
SELECT c0 FROM file(currentDatabase() || '_04064_nested.orc', 'ORC', 'c0 Nullable(Tuple(Tuple(UInt32, String), UInt64))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_nested;

-- Schema inference without type hint
DROP TABLE IF EXISTS test_nullable_tuple_infer;
CREATE TABLE test_nullable_tuple_infer (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_infer VALUES ((1, 'a')), (NULL), ((3, 'c'));

-- Arrow infer
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_infer.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_infer;
SELECT c0 FROM file(currentDatabase() || '_04064_infer.arrow', 'Arrow');

-- ORC infer
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_infer.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_infer;
SELECT c0 FROM file(currentDatabase() || '_04064_infer.orc', 'ORC');

-- ORC legacy infer
SELECT c0 FROM file(currentDatabase() || '_04064_infer.orc', 'ORC') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_infer;

-- Named tuple
DROP TABLE IF EXISTS test_nullable_tuple_named;
CREATE TABLE test_nullable_tuple_named (c0 Nullable(Tuple(a UInt32, b String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_named VALUES ((1, 'x')), (NULL), ((3, 'z'));

-- Arrow named
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_named.arrow', 'Arrow', 'c0 Nullable(Tuple(a UInt32, b String))') SELECT c0 FROM test_nullable_tuple_named;
SELECT c0 FROM file(currentDatabase() || '_04064_named.arrow', 'Arrow', 'c0 Nullable(Tuple(a UInt32, b String))');

-- ORC named
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_named.orc', 'ORC', 'c0 Nullable(Tuple(a UInt32, b String))') SELECT c0 FROM test_nullable_tuple_named;
SELECT c0 FROM file(currentDatabase() || '_04064_named.orc', 'ORC', 'c0 Nullable(Tuple(a UInt32, b String))');

-- ORC legacy named
SELECT c0 FROM file(currentDatabase() || '_04064_named.orc', 'ORC', 'c0 Nullable(Tuple(a UInt32, b String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_named;

-- All-NULL column
DROP TABLE IF EXISTS test_nullable_tuple_allnull;
CREATE TABLE test_nullable_tuple_allnull (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_allnull VALUES (NULL), (NULL), (NULL);

-- Arrow all null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_allnull.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_allnull;
SELECT c0 FROM file(currentDatabase() || '_04064_allnull.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC all null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_allnull.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_allnull;
SELECT c0 FROM file(currentDatabase() || '_04064_allnull.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC legacy all null
SELECT c0 FROM file(currentDatabase() || '_04064_allnull.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_allnull;

-- No-NULL column (nullable type, zero actual NULLs)
DROP TABLE IF EXISTS test_nullable_tuple_nonull;
CREATE TABLE test_nullable_tuple_nonull (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_nonull VALUES ((1, 'a')), ((2, 'b')), ((3, 'c'));

-- Arrow no null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_nonull.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_nonull;
SELECT c0 FROM file(currentDatabase() || '_04064_nonull.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC no null
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_nonull.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SELECT c0 FROM test_nullable_tuple_nonull;
SELECT c0 FROM file(currentDatabase() || '_04064_nonull.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC legacy no null
SELECT c0 FROM file(currentDatabase() || '_04064_nonull.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_nonull;

-- Single-element tuple
DROP TABLE IF EXISTS test_nullable_tuple_single;
CREATE TABLE test_nullable_tuple_single (c0 Nullable(Tuple(UInt32))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_single VALUES ((1,)), (NULL), ((3,));

-- Arrow single
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_single.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32))') SELECT c0 FROM test_nullable_tuple_single;
SELECT c0 FROM file(currentDatabase() || '_04064_single.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32))');

-- ORC single
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_single.orc', 'ORC', 'c0 Nullable(Tuple(UInt32))') SELECT c0 FROM test_nullable_tuple_single;
SELECT c0 FROM file(currentDatabase() || '_04064_single.orc', 'ORC', 'c0 Nullable(Tuple(UInt32))');

-- ORC legacy single
SELECT c0 FROM file(currentDatabase() || '_04064_single.orc', 'ORC', 'c0 Nullable(Tuple(UInt32))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_single;

-- Deeply nested: nullable tuple inside nullable tuple
DROP TABLE IF EXISTS test_nullable_tuple_deep;
CREATE TABLE test_nullable_tuple_deep (c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_deep VALUES (((1, 'a'), 10)), (NULL), ((NULL, 20)), (((4, 'd'), 40));

-- Arrow deep nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_deep.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_deep;
SELECT c0 FROM file(currentDatabase() || '_04064_deep.arrow', 'Arrow', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))');

-- ORC deep nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_deep.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_deep;
SELECT c0 FROM file(currentDatabase() || '_04064_deep.orc', 'ORC', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))');

-- ORC legacy deep nested
SELECT c0 FROM file(currentDatabase() || '_04064_deep.orc', 'ORC', 'c0 Nullable(Tuple(Nullable(Tuple(UInt32, String)), UInt64))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_deep;

-- Nullable tuple with Array element
DROP TABLE IF EXISTS test_nullable_tuple_arr;
CREATE TABLE test_nullable_tuple_arr (c0 Nullable(Tuple(Array(UInt32), String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_arr VALUES (([1, 2], 'a')), (NULL), (([3], 'c'));

-- Arrow array elem
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_arr.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_arr;
SELECT c0 FROM file(currentDatabase() || '_04064_arr.arrow', 'Arrow', 'c0 Nullable(Tuple(Array(UInt32), String))');

-- ORC array elem
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_arr.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_arr;
SELECT c0 FROM file(currentDatabase() || '_04064_arr.orc', 'ORC', 'c0 Nullable(Tuple(Array(UInt32), String))');

-- ORC legacy array elem
SELECT c0 FROM file(currentDatabase() || '_04064_arr.orc', 'ORC', 'c0 Nullable(Tuple(Array(UInt32), String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_arr;

-- Multiple nullable tuple columns
DROP TABLE IF EXISTS test_nullable_tuple_multi;
CREATE TABLE test_nullable_tuple_multi (c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_multi VALUES ((1, 'a'), (1.5)), (NULL, (2.5)), ((3, 'c'), NULL);

-- Arrow multi col
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_multi.arrow', 'Arrow') SELECT c0, c1 FROM test_nullable_tuple_multi;
SELECT c0, c1 FROM file(currentDatabase() || '_04064_multi.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))');

-- ORC multi col
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_multi.orc', 'ORC') SELECT c0, c1 FROM test_nullable_tuple_multi;
SELECT c0, c1 FROM file(currentDatabase() || '_04064_multi.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))');

-- ORC legacy multi col
SELECT c0, c1 FROM file(currentDatabase() || '_04064_multi.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String)), c1 Nullable(Tuple(Float64))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_multi;

-- Type hint mismatch: file has Nullable(Tuple(...)), read as Tuple(...) (strip nullable, NULLs become defaults)
DROP TABLE IF EXISTS test_nullable_tuple_mismatch1;
CREATE TABLE test_nullable_tuple_mismatch1 (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_mismatch1 VALUES ((1, 'a')), (NULL), ((3, 'c'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_mismatch1.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_mismatch1;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_mismatch1.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_mismatch1;

-- Arrow: read nullable file as non-nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_mismatch1.arrow', 'Arrow', 'c0 Tuple(UInt32, String)');

-- ORC: read nullable file as non-nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_mismatch1.orc', 'ORC', 'c0 Tuple(UInt32, String)');

-- ORC legacy: read nullable file as non-nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_mismatch1.orc', 'ORC', 'c0 Tuple(UInt32, String)') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_mismatch1;

-- Type hint mismatch: file has Tuple(...), read as Nullable(Tuple(...)) (add nullable wrapper)
DROP TABLE IF EXISTS test_nullable_tuple_mismatch2;
CREATE TABLE test_nullable_tuple_mismatch2 (c0 Tuple(UInt32, String)) ENGINE = Memory;
INSERT INTO test_nullable_tuple_mismatch2 VALUES ((1, 'a')), ((2, 'b'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_mismatch2.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_mismatch2;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_mismatch2.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_mismatch2;

-- Arrow: read non-nullable file as nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_mismatch2.arrow', 'Arrow', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC: read non-nullable file as nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_mismatch2.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))');

-- ORC legacy: read non-nullable file as nullable
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_mismatch2.orc', 'ORC', 'c0 Nullable(Tuple(UInt32, String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_mismatch2;

-- Schema inference: DESCRIBE without type hint shows inferred type
DROP TABLE IF EXISTS test_nullable_tuple_describe;
CREATE TABLE test_nullable_tuple_describe (c0 Nullable(Tuple(UInt32, String))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_describe VALUES ((1, 'a')), (NULL), ((3, 'c'));

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_describe.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_describe;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_describe.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_describe;

-- Arrow: inferred type
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_describe.arrow', 'Arrow');

-- ORC: inferred type
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_describe.orc', 'ORC');

DROP TABLE test_nullable_tuple_describe;

-- Array(Nullable(Tuple)) flattened via import_nested: struct-level NULLs should propagate to elements
DROP TABLE IF EXISTS test_nullable_tuple_import_nested;
CREATE TABLE test_nullable_tuple_import_nested (c0 Array(Nullable(Tuple(a UInt32, b String)))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_import_nested VALUES ([(1, 'a'), NULL, (3, 'c')]);

-- Arrow import_nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_import_nested.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_import_nested;
SELECT * FROM file(currentDatabase() || '_04064_import_nested.arrow', 'Arrow', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Nullable(String))') SETTINGS input_format_arrow_import_nested = 1;

-- ORC import_nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_import_nested.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_import_nested;
SELECT * FROM file(currentDatabase() || '_04064_import_nested.orc', 'ORC', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Nullable(String))') SETTINGS input_format_orc_import_nested = 1;

-- ORC legacy import_nested
SELECT * FROM file(currentDatabase() || '_04064_import_nested.orc', 'ORC', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Nullable(String))') SETTINGS input_format_orc_import_nested = 1, input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_import_nested;

-- Array(Nullable(Tuple)) without named elements: round-trip as a single column, no flattening
DROP TABLE IF EXISTS test_nullable_tuple_arr_unnamed;
CREATE TABLE test_nullable_tuple_arr_unnamed (c0 Array(Nullable(Tuple(UInt32, String)))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_arr_unnamed VALUES ([(1, 'a'), NULL, (3, 'c')]);

-- Arrow unnamed
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_arr_unnamed.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_arr_unnamed;
SELECT c0 FROM file(currentDatabase() || '_04064_arr_unnamed.arrow', 'Arrow', 'c0 Array(Nullable(Tuple(UInt32, String)))');

-- ORC unnamed
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_arr_unnamed.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_arr_unnamed;
SELECT c0 FROM file(currentDatabase() || '_04064_arr_unnamed.orc', 'ORC', 'c0 Array(Nullable(Tuple(UInt32, String)))');

-- ORC legacy unnamed
SELECT c0 FROM file(currentDatabase() || '_04064_arr_unnamed.orc', 'ORC', 'c0 Array(Nullable(Tuple(UInt32, String)))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_arr_unnamed;

-- Array(Nullable(Tuple)) with Array element inside: import_nested flattens, Array defaults to [] at null positions
DROP TABLE IF EXISTS test_nullable_tuple_arr_nested_elem;
CREATE TABLE test_nullable_tuple_arr_nested_elem (c0 Array(Nullable(Tuple(a UInt32, b Array(UInt32))))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_arr_nested_elem VALUES ([(1, [10, 20]), NULL, (3, [30])]);

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_arr_nested_elem.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_arr_nested_elem;

-- Arrow import_nested: scalar becomes Nullable, Array defaults to [] at null struct positions
SELECT * FROM file(currentDatabase() || '_04064_arr_nested_elem.arrow', 'Arrow', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Array(UInt32))') SETTINGS input_format_arrow_import_nested = 1;

-- ORC import_nested
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_arr_nested_elem.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_arr_nested_elem;
SELECT * FROM file(currentDatabase() || '_04064_arr_nested_elem.orc', 'ORC', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Array(UInt32))') SETTINGS input_format_orc_import_nested = 1;

-- ORC legacy import_nested
SELECT * FROM file(currentDatabase() || '_04064_arr_nested_elem.orc', 'ORC', '`c0.a` Array(Nullable(UInt32)), `c0.b` Array(Array(UInt32))') SETTINGS input_format_orc_import_nested = 1, input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_arr_nested_elem;

-- LowCardinality(Nullable(String)) hint with no physical nulls in the file: the ORC reader must still wrap the column as nullable
DROP TABLE IF EXISTS test_nullable_tuple_lc_string;
CREATE TABLE test_nullable_tuple_lc_string (c0 String) ENGINE = Memory;
INSERT INTO test_nullable_tuple_lc_string VALUES ('hello'), ('world');

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_lc_str.arrow', 'Arrow') SELECT c0 FROM test_nullable_tuple_lc_string;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04064_lc_str.orc', 'ORC') SELECT c0 FROM test_nullable_tuple_lc_string;

-- Arrow: no physical nulls, LowCardinality(Nullable(String)) hint
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_lc_str.arrow', 'Arrow', 'c0 LowCardinality(Nullable(String))');

-- ORC: no physical nulls, LowCardinality(Nullable(String)) hint
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_lc_str.orc', 'ORC', 'c0 LowCardinality(Nullable(String))');

-- ORC legacy: no physical nulls, LowCardinality(Nullable(String)) hint
SELECT c0, toTypeName(c0) FROM file(currentDatabase() || '_04064_lc_str.orc', 'ORC', 'c0 LowCardinality(Nullable(String))') SETTINGS input_format_orc_use_fast_decoder = 0;

DROP TABLE test_nullable_tuple_lc_string;
