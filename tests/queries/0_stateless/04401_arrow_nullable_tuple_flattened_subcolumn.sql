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
-- under case-insensitive column matching. A requested name that is no element name of its own must
-- still match a declared one case-insensitively; otherwise the genuine NULL is lost (collapsed to a
-- default tuple) at allow_nullable_tuple_in_extracted_subcolumns=0.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_ci.arrow', 'Arrow')
SELECT c0 FROM values('c0 Tuple(A Nullable(Tuple(b Nullable(UInt32))), C String)', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.a`, isNull(`c0.a`), `c0.c` FROM file(currentDatabase() || '_04401_ci.arrow', 'Arrow', '`c0.a` Nullable(Tuple(b Nullable(UInt32))), `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_ci.orc', 'ORC')
SELECT c0 FROM values('c0 Tuple(A Nullable(Tuple(b Nullable(UInt32))), C String)', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.a`, isNull(`c0.a`), `c0.c` FROM file(currentDatabase() || '_04401_ci.orc', 'ORC', '`c0.a` Nullable(Tuple(b Nullable(UInt32))), `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_orc_case_insensitive_column_matching = 1;

-- A Map root is addressed the same way, so its virtual `keys`/`values` subcolumns are readable
-- whether or not the requested structure happens to name both of them.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_map.arrow', 'Arrow') SELECT map('a', 1, 'b', 2) AS m;
SELECT `m.keys`, `m.values` FROM file(currentDatabase() || '_04401_map.arrow', 'Arrow');
SELECT `m.keys` FROM file(currentDatabase() || '_04401_map.arrow', 'Arrow', '`m.keys` Array(String)');
INSERT INTO FUNCTION file(currentDatabase() || '_04401_map.arrowstream', 'ArrowStream') SELECT map('a', 1, 'b', 2) AS m;
SELECT `m.keys`, `m.values` FROM file(currentDatabase() || '_04401_map.arrowstream', 'ArrowStream');
SELECT `m.keys` FROM file(currentDatabase() || '_04401_map.arrowstream', 'ArrowStream', '`m.keys` Array(String)');

-- Sibling subcolumns whose requested root spellings differ are the same root under case-insensitive
-- matching, and each is resolved against that root on its own.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_cisib.arrow', 'Arrow')
SELECT CAST([(1, 'a'), (2, 'b')], 'Array(Tuple(x UInt32, y String))') AS c0;
SELECT `C0.x`, `c0.y` FROM file(currentDatabase() || '_04401_cisib.arrow', 'Arrow', '`C0.x` Array(UInt32), `c0.y` Array(String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT `C0.x`, `c0.y` FROM file(currentDatabase() || '_04401_cisib.arrow', 'Arrow', '`C0.x` Array(UInt32), `c0.y` Array(String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 0;
SELECT `M.keys`, `m.values` FROM file(currentDatabase() || '_04401_map.arrow', 'Arrow', '`M.keys` Array(String), `m.values` Array(UInt32)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

-- A dotted name that is itself a field of the file is read from that field, not as a sibling of a
-- struct root of the same name, whether or not the two spellings match.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_cicoll.arrow', 'Arrow')
SELECT CAST([('nested-x', 'nested-y')], 'Array(Tuple(x String, y String))') AS c0, [7]::Array(UInt32) AS `C0.x`;
SELECT `C0.x`, `c0.y` FROM file(currentDatabase() || '_04401_cicoll.arrow', 'Arrow', '`C0.x` Array(UInt32), `c0.y` Array(String)') SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_coll.arrow', 'Arrow')
SELECT CAST([('nested-x', 'nested-y')], 'Array(Tuple(x String, y String))') AS c0, [7]::Array(UInt32) AS `c0.x`;
SELECT `c0.x`, `c0.y` FROM file(currentDatabase() || '_04401_coll.arrow', 'Arrow', '`c0.x` Array(UInt32), `c0.y` Array(String)');

-- A tuple element name may itself contain a dot, so `c0.a.b` names both the element `a.b` and the
-- path from `a` to `b`. Reading the flattened name must pick whichever of the two a direct
-- `SELECT c0.<name>` picks, which is the declared element.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_dot.arrow', 'Arrow')
SELECT c0 FROM values('c0 Tuple(`a.b` UInt32, a Tuple(b Nullable(Tuple(v UInt32))), c String)', (tuple(1, tuple(tuple(10)), 'p')), (tuple(2, tuple(NULL), 'q')), (tuple(3, tuple(tuple(30)), 'r')));
SELECT `c0.a.b`, `c0.c` FROM file(currentDatabase() || '_04401_dot.arrow', 'Arrow', '`c0.a.b` UInt32, `c0.c` String') ORDER BY `c0.c`;
SELECT c0.`a.b`, c0.c FROM file(currentDatabase() || '_04401_dot.arrow', 'Arrow', 'c0 Tuple(`a.b` UInt32, a Tuple(b Nullable(Tuple(v UInt32))), c String)') ORDER BY c0.c;

-- The ORC reader resolves the dotted name against the file's struct itself, and folding the names
-- case-insensitively does not reorder the two candidates either.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_dot.orc', 'ORC')
SELECT c0 FROM values('c0 Tuple(`a.b` UInt32, a Tuple(b UInt32), c String)', (tuple(1, tuple(101), 'p')), (tuple(2, tuple(102), 'q')), (tuple(3, tuple(103), 'r')));
SELECT `c0.a.b`, `c0.c` FROM file(currentDatabase() || '_04401_dot.orc', 'ORC', '`c0.a.b` UInt32, `c0.c` String') ORDER BY `c0.c`;
SELECT `c0.a.b`, `c0.c` FROM file(currentDatabase() || '_04401_dot.orc', 'ORC', '`c0.a.b` UInt32, `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT c0.`a.b`, c0.c FROM file(currentDatabase() || '_04401_dot.orc', 'ORC', 'c0 Tuple(`a.b` UInt32, a Tuple(b UInt32), c String)') ORDER BY c0.c;

-- Which of the two candidates that is depends on the order the schema declares them in, so where the
-- struct root comes first, compare the two reads instead of pinning either candidate's values.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_dotrev.orc', 'ORC')
SELECT c0 FROM values('c0 Tuple(a Tuple(b UInt32), `a.b` UInt32, c String)', (tuple(tuple(101), 1, 'p')), (tuple(tuple(102), 2, 'q')), (tuple(tuple(103), 3, 'r')));
SELECT
    (SELECT count() FROM file(currentDatabase() || '_04401_dotrev.orc', 'ORC', '`c0.a.b` UInt32, `c0.c` String')),
    (SELECT count() FROM (SELECT `c0.a.b` FROM file(currentDatabase() || '_04401_dotrev.orc', 'ORC', '`c0.a.b` UInt32, `c0.c` String')
                          EXCEPT
                          SELECT c0.`a.b` FROM file(currentDatabase() || '_04401_dotrev.orc', 'ORC', 'c0 Tuple(a Tuple(b UInt32), `a.b` UInt32, c String)')));

-- Only the exact lowercase `null` is reserved as an element name, so `Null` is legal, and a request
-- for either it or the parent's virtual null-map subcolumn resolves the same way a direct
-- `SELECT c0.<name>` does: exactly for `Null`, and to the null map for `null`.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_nullname.arrow', 'Arrow')
SELECT c0 FROM values('c0 Nullable(Tuple(`Null` Nullable(Tuple(v UInt32)), c String))', (tuple(tuple(10), 'p')), (tuple(NULL, 'q')), (tuple(tuple(30), 'r')));
SELECT `c0.Null`, isNull(`c0.Null`), `c0.c` FROM file(currentDatabase() || '_04401_nullname.arrow', 'Arrow', '`c0.Null` Nullable(Tuple(v UInt32)), `c0.c` Nullable(String)') ORDER BY `c0.c`;
SELECT c0.`Null`, isNull(c0.`Null`), c0.c FROM file(currentDatabase() || '_04401_nullname.arrow', 'Arrow', 'c0 Nullable(Tuple(`Null` Nullable(Tuple(v UInt32)), c String))') ORDER BY c0.c;
SELECT `c0.null`, `c0.c` FROM file(currentDatabase() || '_04401_nullname.arrow', 'Arrow', '`c0.null` UInt8, `c0.c` Nullable(String)') ORDER BY `c0.c` SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
SELECT c0.null, c0.c FROM file(currentDatabase() || '_04401_nullname.arrow', 'Arrow', 'c0 Nullable(Tuple(`Null` Nullable(Tuple(v UInt32)), c String))') ORDER BY c0.c;

-- Case-insensitive column matching folds `Null` and the parent's virtual `null` onto one spelling,
-- so the declared element must keep winning its own exact name rather than the parent's null map,
-- for every reader that resolves a flattened name.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_nullci.arrow', 'Arrow')
SELECT c0 FROM values('c0 Nullable(Tuple(`Null` UInt8, c String))', (tuple(7, 'p')), (NULL), (tuple(9, 'r')));
SELECT `c0.Null`, `c0.c` FROM file(currentDatabase() || '_04401_nullci.arrow', 'Arrow', '`c0.Null` UInt8, `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_arrow_case_insensitive_column_matching = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_nullci.orc', 'ORC')
SELECT c0 FROM values('c0 Nullable(Tuple(`Null` UInt8, c String))', (tuple(7, 'p')), (NULL), (tuple(9, 'r')));
SELECT `c0.Null`, `c0.c` FROM file(currentDatabase() || '_04401_nullci.orc', 'ORC', '`c0.Null` UInt8, `c0.c` String') ORDER BY `c0.c` SETTINGS input_format_orc_case_insensitive_column_matching = 1;

-- An element may be named like a virtual subcolumn of its own parent, so `size0` here is both a
-- declared element and the array's length. The flattened read resolves it the way a direct
-- `SELECT c0.size0` does, which is the length; a sibling element still reads its own data.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_size0.arrow', 'Arrow')
SELECT CAST([(1, 'a'), (2, 'b')], 'Array(Tuple(size0 UInt32, x String))') AS c0;
SELECT `c0.size0`, `c0.x` FROM file(currentDatabase() || '_04401_size0.arrow', 'Arrow', '`c0.size0` UInt64, `c0.x` Array(String)');
SELECT c0.size0, c0.x FROM file(currentDatabase() || '_04401_size0.arrow', 'Arrow', 'c0 Array(Tuple(size0 UInt32, x String))');

-- The flattened read and the direct subcolumn read go through the same function, so they agree on
-- the reported case too.
SELECT `s.v` FROM file(currentDatabase() || '_04401.arrow', 'Arrow', '`s.v` Int32');
SELECT s.v FROM file(currentDatabase() || '_04401.arrow', 'Arrow', 's Nullable(Tuple(v Int32))');

-- A struct-NULL row in an Arrow file may carry non-default child values; what an extracted subcolumn
-- reports for such a row is decided by the subcolumn path, so assert only that the flattened read
-- agrees with the direct one there. `tupleElement` over the whole struct reports type defaults.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_hidden.arrow', 'Arrow')
SELECT id, if(id = 2, NULL, p)::Nullable(Tuple(a Tuple(x UInt32), arr Array(UInt32), m Map(String, String))) AS n
FROM values('id UInt8, p Tuple(a Tuple(x UInt32), arr Array(UInt32), m Map(String, String))', (1, tuple(tuple(10), [1], map('k1', 'v1'))), (2, tuple(tuple(99), [3], map('k2', 'v2'))));
SELECT id, `n.a`, `n.arr`, `n.m` FROM file(currentDatabase() || '_04401_hidden.arrow', 'Arrow', 'id UInt8, `n.a` Tuple(x UInt32), `n.arr` Array(UInt32), `n.m` Map(String, String)') WHERE id = 1;
SELECT
    (SELECT count() FROM file(currentDatabase() || '_04401_hidden.arrow', 'Arrow', 'id UInt8, `n.a` Tuple(x UInt32), `n.arr` Array(UInt32), `n.m` Map(String, String)')),
    (SELECT count() FROM (SELECT id, `n.a`, `n.arr`, `n.m` FROM file(currentDatabase() || '_04401_hidden.arrow', 'Arrow', 'id UInt8, `n.a` Tuple(x UInt32), `n.arr` Array(UInt32), `n.m` Map(String, String)')
                          EXCEPT
                          SELECT id, n.a, n.arr, n.m FROM file(currentDatabase() || '_04401_hidden.arrow', 'Arrow', 'id UInt8, n Nullable(Tuple(a Tuple(x UInt32), arr Array(UInt32), m Map(String, String)))')));
SELECT id, tupleElement(materialize(n), 'a'), tupleElement(materialize(n), 'arr'), tupleElement(materialize(n), 'm') FROM file(currentDatabase() || '_04401_hidden.arrow', 'Arrow', 'id UInt8, n Nullable(Tuple(a Tuple(x UInt32), arr Array(UInt32), m Map(String, String)))') ORDER BY id;

-- Case-insensitive column matching folds a struct's field names, so a struct holding two fields
-- whose names differ only by case has two candidates for one request. The field spelled exactly like
-- the request wins, for the flattened read and for its sibling, and for the whole column in the ORC
-- reader, which resolves each requested name against the file's struct itself.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_cicase.orc', 'ORC')
SELECT id, c0 FROM values('id UInt32, c0 Tuple(A Nullable(Tuple(b UInt32)), a Tuple(b UInt32))', (1, tuple(tuple(11), tuple(21))), (2, tuple(NULL, tuple(22))), (3, tuple(tuple(13), tuple(23))));
SELECT id, `c0.a` FROM file(currentDatabase() || '_04401_cicase.orc', 'ORC', 'id UInt32, `c0.a` Tuple(b UInt32)') ORDER BY id SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT id, `c0.A` FROM file(currentDatabase() || '_04401_cicase.orc', 'ORC', 'id UInt32, `c0.A` Tuple(b UInt32)') ORDER BY id SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT id, c0 FROM file(currentDatabase() || '_04401_cicase.orc', 'ORC', 'id UInt32, c0 Tuple(A Nullable(Tuple(b UInt32)), a Tuple(b UInt32))') ORDER BY id SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT id, `c0.a` FROM file(currentDatabase() || '_04401_cicase.orc', 'ORC', 'id UInt32, `c0.a` Tuple(b UInt32)') ORDER BY id SETTINGS input_format_orc_case_insensitive_column_matching = 0;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_cicase.arrow', 'Arrow')
SELECT id, c0 FROM values('id UInt32, c0 Tuple(A Nullable(Tuple(b UInt32)), a Tuple(b UInt32))', (1, tuple(tuple(11), tuple(21))), (2, tuple(NULL, tuple(22))), (3, tuple(tuple(13), tuple(23))));
SELECT id, `c0.a` FROM file(currentDatabase() || '_04401_cicase.arrow', 'Arrow', 'id UInt32, `c0.a` Tuple(b UInt32)') ORDER BY id SETTINGS input_format_arrow_case_insensitive_column_matching = 1;

-- A request that matches no field exactly is still resolved case-insensitively, which is what the
-- setting is for.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_cifold.orc', 'ORC') SELECT CAST(tuple(tuple(70)), 'Tuple(UP Tuple(B UInt32))') AS c0;
SELECT `c0.up` FROM file(currentDatabase() || '_04401_cifold.orc', 'ORC', '`c0.up` Tuple(b UInt32)') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT c0 FROM file(currentDatabase() || '_04401_cifold.orc', 'ORC', 'c0 Tuple(up Tuple(b UInt32))') SETTINGS input_format_orc_case_insensitive_column_matching = 1;

-- Top-level file columns are resolved by the same rule, and which of two columns differing only by
-- case answers a request must not depend on the order the file lists them in.
INSERT INTO FUNCTION file(currentDatabase() || '_04401_citop.orc', 'ORC') SELECT 10 AS A, 20 AS a, 70 AS UP;
SELECT A, a FROM file(currentDatabase() || '_04401_citop.orc', 'ORC', 'A UInt32, a UInt32') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
SELECT up FROM file(currentDatabase() || '_04401_citop.orc', 'ORC', 'up UInt32') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04401_citop_rev.orc', 'ORC') SELECT 20 AS a, 10 AS A;
SELECT A, a FROM file(currentDatabase() || '_04401_citop_rev.orc', 'ORC', 'A UInt32, a UInt32') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
