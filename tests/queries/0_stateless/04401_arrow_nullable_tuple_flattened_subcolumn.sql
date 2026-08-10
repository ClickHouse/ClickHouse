-- Tags: no-fasttest
-- no-fasttest: Arrow and ORC formats are not available in fasttest builds
-- Reading a flattened Tuple subcolumn (e.g. `s.v`) from an Arrow/ArrowStream/ORC file whose
-- column is a nullable struct must return the nested data, not column defaults.
-- Regression for https://github.com/ClickHouse/ClickHouse/issues/109726 (caused by #101272).

-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;

-- Single-field nullable struct, flattened subcolumn read (the reported case: expected 42, not 0)
INSERT INTO FUNCTION file(currentDatabase() || '_04401.arrow', 'Arrow') SELECT CAST(tuple(42), 'Nullable(Tuple(v Int32))') AS s;
SELECT * FROM file(currentDatabase() || '_04401.arrow', 'Arrow', '`s.v` Int32');
INSERT INTO FUNCTION file(currentDatabase() || '_04401.arrowstream', 'ArrowStream') SELECT CAST(tuple(42), 'Nullable(Tuple(v Int32))') AS s;
SELECT * FROM file(currentDatabase() || '_04401.arrowstream', 'ArrowStream', '`s.v` Int32');

-- Multiple rows, multiple fields, with a NULL struct row
INSERT INTO FUNCTION file(currentDatabase() || '_04401_multi.arrow', 'Arrow')
SELECT c0 FROM values('c0 Nullable(Tuple(a UInt32, b String))', (1, 'x'), NULL, (3, 'z'));
SELECT `c0.a`, `c0.b` FROM file(currentDatabase() || '_04401_multi.arrow', 'Arrow', '`c0.a` UInt32, `c0.b` String') ORDER BY `c0.a`;

-- Read the subcolumn as Nullable so the struct null is preserved instead of turning into a default
SELECT `c0.a` FROM file(currentDatabase() || '_04401_multi.arrow', 'Arrow', '`c0.a` Nullable(UInt32)') ORDER BY `c0.a` NULLS LAST;

-- Nested tuple inside nullable struct: deep flattened subcolumn read
INSERT INTO FUNCTION file(currentDatabase() || '_04401_nested.arrow', 'Arrow')
SELECT c0 FROM values('c0 Nullable(Tuple(inner Tuple(x UInt32, y String), z UInt64))', ((10, 'a'), 100), NULL, ((30, 'c'), 300));
SELECT `c0.inner.x`, `c0.inner.y`, `c0.z` FROM file(currentDatabase() || '_04401_nested.arrow', 'Arrow', '`c0.inner.x` UInt32, `c0.inner.y` String, `c0.z` UInt64') ORDER BY `c0.inner.x`;

-- Full-column read still works (baseline, unchanged)
SELECT s.v FROM file(currentDatabase() || '_04401.arrow', 'Arrow', 's Nullable(Tuple(v Int32))');

-- ORC is affected the same way as Arrow: exercise the same cases through the ORC reader.
INSERT INTO FUNCTION file(currentDatabase() || '_04401.orc', 'ORC') SELECT CAST(tuple(42), 'Nullable(Tuple(v Int32))') AS s;
SELECT * FROM file(currentDatabase() || '_04401.orc', 'ORC', '`s.v` Int32');

INSERT INTO FUNCTION file(currentDatabase() || '_04401_multi.orc', 'ORC')
SELECT c0 FROM values('c0 Nullable(Tuple(a UInt32, b String))', (1, 'x'), NULL, (3, 'z'));
SELECT `c0.a`, `c0.b` FROM file(currentDatabase() || '_04401_multi.orc', 'ORC', '`c0.a` UInt32, `c0.b` String') ORDER BY `c0.a`;
SELECT `c0.a` FROM file(currentDatabase() || '_04401_multi.orc', 'ORC', '`c0.a` Nullable(UInt32)') ORDER BY `c0.a` NULLS LAST;

INSERT INTO FUNCTION file(currentDatabase() || '_04401_nested.orc', 'ORC')
SELECT c0 FROM values('c0 Nullable(Tuple(inner Tuple(x UInt32, y String), z UInt64))', ((10, 'a'), 100), NULL, ((30, 'c'), 300));
SELECT `c0.inner.x`, `c0.inner.y`, `c0.z` FROM file(currentDatabase() || '_04401_nested.orc', 'ORC', '`c0.inner.x` UInt32, `c0.inner.y` String, `c0.z` UInt64') ORDER BY `c0.inner.x`;

SELECT s.v FROM file(currentDatabase() || '_04401.orc', 'ORC', 's Nullable(Tuple(v Int32))');

-- Empty Nullable(Tuple()) with a missing-column subcolumn hint must not throw a logical error.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_empty.arrow', 'Arrow') SELECT CAST(tuple(), 'Nullable(Tuple())') AS s;
SELECT * FROM file(currentDatabase() || '_04401_empty.arrow', 'Arrow', '`s.x` Int32') SETTINGS input_format_arrow_allow_missing_columns = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_empty.orc', 'ORC') SELECT CAST(tuple(), 'Nullable(Tuple())') AS s;
SELECT * FROM file(currentDatabase() || '_04401_empty.orc', 'ORC', '`s.x` Int32') SETTINGS input_format_orc_allow_missing_columns = 1;

-- A genuinely-declared Nullable(Tuple) descendant inside a non-nullable struct must keep its real
-- NULL rows (\N) when read as a subcolumn, not collapse them to a default tuple. Only a synthetic
-- Nullable(Tuple) wrapping (from an outer struct null map) follows allow_nullable_tuple_in_extracted_subcolumns.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_gen.arrow', 'Arrow')
SELECT c0 FROM values('c0 Tuple(a Nullable(Tuple(b Nullable(UInt32))), c String)', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.a`, isNull(`c0.a`), `c0.c` FROM file(currentDatabase() || '_04401_gen.arrow', 'Arrow', '`c0.a` Nullable(Tuple(b Nullable(UInt32))), `c0.c` String') ORDER BY `c0.c`;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_gen.orc', 'ORC')
SELECT c0 FROM values('c0 Tuple(a Nullable(Tuple(b Nullable(UInt32))), c String)', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.a`, isNull(`c0.a`), `c0.c` FROM file(currentDatabase() || '_04401_gen.orc', 'ORC', '`c0.a` Nullable(Tuple(b Nullable(UInt32))), `c0.c` String') ORDER BY `c0.c`;

-- Same genuinely-declared Nullable(Tuple) descendant but with a mixed-case declared element (A) read
-- under case-insensitive column matching. The reader lowercases the requested name before the
-- declared-type lookup, so the lookup must match the declared element name case-insensitively;
-- otherwise the genuine NULL is lost (collapsed to a default tuple) at
-- allow_nullable_tuple_in_extracted_subcolumns=0.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_ci.arrow', 'Arrow')
SELECT c0 FROM values('c0 Tuple(A Nullable(Tuple(b Nullable(UInt32))), C String)', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.a`, isNull(`c0.a`), `c0.c` FROM file(currentDatabase() || '_04401_ci.arrow', 'Arrow', '`c0.a` Nullable(Tuple(b Nullable(UInt32))), `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_ci.orc', 'ORC')
SELECT c0 FROM values('c0 Tuple(A Nullable(Tuple(b Nullable(UInt32))), C String)', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.a`, isNull(`c0.a`), `c0.c` FROM file(currentDatabase() || '_04401_ci.orc', 'ORC', '`c0.a` Nullable(Tuple(b Nullable(UInt32))), `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_orc_case_insensitive_column_matching = 1;

-- Only a top-level struct may skip the Nested reshape. A Map root still needs it: with the requested
-- structure naming just the subcolumn, skipping the reshape makes the extractor find no column and
-- `input_format_arrow_allow_missing_columns` then yields a silent empty array instead of this error.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_map.arrow', 'Arrow') SELECT map('a', 1, 'b', 2) AS m;
SELECT `m.keys`, `m.values` FROM file(currentDatabase() || '_04401_map.arrow', 'Arrow');
SELECT `m.keys` FROM file(currentDatabase() || '_04401_map.arrow', 'Arrow', '`m.keys` Array(String)'); -- { serverError TYPE_MISMATCH }
INSERT INTO FUNCTION file(currentDatabase() || '_04401_map.arrowstream', 'ArrowStream') SELECT map('a', 1, 'b', 2) AS m;
SELECT `m.keys`, `m.values` FROM file(currentDatabase() || '_04401_map.arrowstream', 'ArrowStream');
SELECT `m.keys` FROM file(currentDatabase() || '_04401_map.arrowstream', 'ArrowStream', '`m.keys` Array(String)'); -- { serverError TYPE_MISMATCH }
