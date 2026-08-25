-- A subcolumn name can be claimed by several substreams. The winner is, in order:
-- a substream the user named in the type, a subcolumn resolved from the data in the same
-- namespace, and only then a name the serialization generated (sizeN, null, size).

SET optimize_functions_to_subcolumns = 1;

SELECT '--- user-named element wins over array sizes ---';

DROP TABLE IF EXISTS t_memory;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;

CREATE TABLE t_memory (c Array(Tuple(`size0` UInt64))) ENGINE = Memory;
CREATE TABLE t_wide (c Array(Tuple(`size0` UInt64))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
CREATE TABLE t_compact (c Array(Tuple(`size0` UInt64))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '10G';

INSERT INTO t_memory VALUES ([(100), (200)]), ([(300)]), ([]);
INSERT INTO t_wide SELECT * FROM t_memory;
INSERT INTO t_compact SELECT * FROM t_memory;

SELECT 'memory', toTypeName(c.size0), c.size0 FROM t_memory ORDER BY ALL;
SELECT 'wide', toTypeName(c.size0), c.size0 FROM t_wide ORDER BY ALL;
SELECT 'compact', toTypeName(c.size0), c.size0 FROM t_compact ORDER BY ALL;

-- The array sizes have no name of their own anymore, but length still works.
SELECT 'length', length(c) FROM t_wide ORDER BY ALL;
SELECT 'length no rewrite', length(c) FROM t_wide ORDER BY ALL SETTINGS optimize_functions_to_subcolumns = 0;

DESCRIBE t_wide SETTINGS describe_include_subcolumns = 1;

DROP TABLE t_memory;
DROP TABLE t_wide;
DROP TABLE t_compact;

SELECT '--- the shadowed element and its own subcolumns are consistent ---';

DROP TABLE IF EXISTS t_tree;
CREATE TABLE t_tree (c Array(Tuple(`size0` String))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_tree VALUES ([('abc')]);
SELECT c.size0, c.size0.size FROM t_tree;
DROP TABLE t_tree;

SELECT '--- JSON path wins over an automatic name in the same namespace ---';

DROP TABLE IF EXISTS t_json;
CREATE TABLE t_json
(
    arr Array(JSON),
    arr2 Array(Array(JSON)),
    tup Tuple(`x` Array(JSON))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_json VALUES ([('{"size0":7}')], [[('{"size0":8}')]], (['{"size0":9}']));

SELECT toTypeName(arr.size0), toString(arr.size0) FROM t_json;
SELECT toTypeName(arr2.size0), toTypeName(arr2.size1) FROM t_json;
SELECT toTypeName(tup.x.size0), toString(tup.x.size0) FROM t_json;

-- The rewrite is not applied when the name means a path, and the result stays correct.
SELECT length(arr), length(arr) FROM t_json SETTINGS optimize_functions_to_subcolumns = 0;
SELECT length(arr), empty(arr), notEmpty(arr) FROM t_json;

DESCRIBE t_json SETTINGS describe_include_subcolumns = 1;
DROP TABLE t_json;

SELECT '--- a declared path keeps its automatic subcolumns ---';

DROP TABLE IF EXISTS t_typed;
CREATE TABLE t_typed (c JSON(`a` Array(Int64), `s` String, `n` Nullable(Int64)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_typed VALUES ('{"a":[1,2],"s":"abc"}');

SELECT toTypeName(c.a.size0), c.a.size0 FROM t_typed;
SELECT toTypeName(c.s.size), c.s.size FROM t_typed;
SELECT toTypeName(c.n.null), c.n.null FROM t_typed;
SELECT length(c.a), empty(c.s), isNull(c.n) FROM t_typed;
DROP TABLE t_typed;

-- Even when a dotted key really put a value at that path, the declaration wins for `a.size0`.
DROP TABLE IF EXISTS t_dotted;
CREATE TABLE t_dotted (c JSON(`a` Array(Int64))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_dotted VALUES ('{"a":[1,2],"a.size0":5}');
SELECT c.a.size0, toString(tupleElement(c, 'a.size0')) FROM t_dotted;
DROP TABLE t_dotted;

SELECT '--- types with no claimant are unaffected ---';

DROP TABLE IF EXISTS t_other;
CREATE TABLE t_other
(
    m Map(String, JSON),
    d Array(Dynamic),
    s Array(String),
    n Nullable(JSON)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_other VALUES ({'k':'{"a":1}'}, [1, 'x'], ['a', 'b'], NULL);

SELECT toTypeName(m.size0), m.size0 FROM t_other;
SELECT toTypeName(d.size0), d.size0 FROM t_other;
SELECT toTypeName(s.size0), s.size0, toTypeName(s.size), s.size FROM t_other;
SELECT length(m), length(d), length(s) FROM t_other;
-- Nullable(JSON).null was already the path before this change.
SELECT toTypeName(n.null), isNull(n) FROM t_other;
DROP TABLE t_other;

SELECT '--- shared Nested offsets ---';

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (n Nested(j JSON, k UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_nested VALUES ([('{"size0":1}'), ('{"size0":2}')], [10, 20]);

SELECT toTypeName(`n.j`.size0), toString(`n.j`.size0) FROM t_nested;
SELECT toTypeName(`n.k`.size0), `n.k`.size0 FROM t_nested;
SELECT length(`n.j`), length(`n.k`) FROM t_nested;
SELECT `n.j`, `n.k` FROM t_nested;
DROP TABLE t_nested;
