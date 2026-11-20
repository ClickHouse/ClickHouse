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

SELECT '--- the most specific declared prefix resolves the rest ---';

DROP TABLE IF EXISTS t_dotted_prefix;
CREATE TABLE t_dotted_prefix (c Tuple(`a` JSON, `a.b` JSON)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_dotted_prefix VALUES (('{"b":{"some":{"path":1}}}', '{"some":{"path":2}}'));

-- `a` can resolve `b.some.path` and `a.b` can resolve `some.path`; the declared `a.b` is the answer.
SELECT c.a.b.some.path FROM t_dotted_prefix;
SELECT toTypeName(c.a.b.some.path) FROM t_dotted_prefix;
DROP TABLE t_dotted_prefix;

SELECT '--- the shared offsets of Nested are read for the array sizes only ---';

-- The arrays of one Nested share a single offsets column, and a request for their sizes is sent to it
-- by the name `size0`. The declared element of the same name must be read from its own data instead.

DROP TABLE IF EXISTS t_nested_wide;
CREATE TABLE t_nested_wide (id UInt32, n Nested(`j` Tuple(`size0` UInt32), `x` String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_nested_wide VALUES (1, [(10), (20)], ['a', 'b']);
SELECT 'wide', toTypeName(n.j.size0), n.j.size0, n.x.size0 FROM t_nested_wide;
DROP TABLE t_nested_wide;

-- A part written before `n.j` was added has no data for it, so the sizes of the group give the
-- default the element takes the shape of.
DROP TABLE IF EXISTS t_nested_compact;
CREATE TABLE t_nested_compact (id UInt32, n Nested(`x` String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G';
INSERT INTO t_nested_compact VALUES (1, ['a', 'b']);
ALTER TABLE t_nested_compact ADD COLUMN `n.j` Array(Tuple(`size0` UInt32));
SELECT 'compact, missed column', toTypeName(n.j.size0), n.j.size0, n.x.size0 FROM t_nested_compact;
DROP TABLE t_nested_compact;

DROP TABLE IF EXISTS t_nested_log;
CREATE TABLE t_nested_log (n Nested(`j` Tuple(`size0` UInt32), `x` String)) ENGINE = Log;
INSERT INTO t_nested_log VALUES ([(10), (20)], ['a', 'b']);
SELECT 'log', toTypeName(n.j.size0), n.j.size0, n.x.size0 FROM t_nested_log;
DROP TABLE t_nested_log;

SELECT '--- compressed Memory reads the declared element, not the array sizes ---';

-- The compressed-Memory fast path decompresses the array offsets for `size0`; it must do so only when
-- `size0` really is the array sizes, not when the element declares the name.
DROP TABLE IF EXISTS t_compressed;
CREATE TABLE t_compressed (c Array(Tuple(`size0` UInt64))) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_compressed VALUES ([(100), (200)]), ([(300)]);
SELECT toTypeName(c.size0), c.size0 FROM t_compressed ORDER BY ALL;
SELECT length(c) FROM t_compressed ORDER BY ALL;
DROP TABLE t_compressed;

-- A plain array still takes the fast path: `size0` is the array sizes.
DROP TABLE IF EXISTS t_compressed_plain;
CREATE TABLE t_compressed_plain (c Array(UInt64)) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_compressed_plain VALUES ([10, 20, 30]), ([40]);
SELECT toTypeName(c.size0), c.size0 FROM t_compressed_plain ORDER BY ALL;
DROP TABLE t_compressed_plain;

SELECT '--- a shorter prefix resolves when the more specific one cannot ---';

-- `a` (JSON) resolves `b.String.x` and `a.b` (Dynamic) matches `String.x` by type name but cannot
-- resolve it. The more specific `a.b` is tried first; when it fails the `a` candidate must remain.
DROP TABLE IF EXISTS t_overlap;
CREATE TABLE t_overlap (c Tuple(`a` JSON, `a.b` Dynamic)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_overlap VALUES (('{"b":{"String":{"x":7}}}', 'abc'));
SELECT c.a.b.String.x FROM t_overlap;
DROP TABLE t_overlap;

SELECT '--- a typed path is declared: the first declaration of a name wins in the listing and in a read ---';

-- A direct read takes the type the listing registered, `getSubcolumn` resolves the name itself; they agree.
DROP TABLE IF EXISTS t_typed_first;
CREATE TABLE t_typed_first (c Tuple(`a` JSON(`b` UInt64), `a.b` String)) ENGINE = Memory;
INSERT INTO t_typed_first VALUES (('{"b":7}', 'x'));
SELECT 'typed path first', toTypeName(c.a.b), c.a.b, toTypeName(getSubcolumn(c, 'a.b')), getSubcolumn(c, 'a.b') FROM t_typed_first;
DROP TABLE t_typed_first;

DROP TABLE IF EXISTS t_element_first;
CREATE TABLE t_element_first (c Tuple(`a.b` String, `a` JSON(`b` UInt64))) ENGINE = Memory;
INSERT INTO t_element_first VALUES (('x', '{"b":7}'));
SELECT 'element first', toTypeName(c.a.b), c.a.b, toTypeName(getSubcolumn(c, 'a.b')), getSubcolumn(c, 'a.b') FROM t_element_first;
DROP TABLE t_element_first;

-- Two typed paths of different elements claim one name.
DROP TABLE IF EXISTS t_two_typed;
CREATE TABLE t_two_typed (c Tuple(`a` JSON(`b.c` UInt8), `a.b` JSON(`c` String))) ENGINE = Memory;
INSERT INTO t_two_typed VALUES (('{"b":{"c":1}}', '{"c":"s"}'));
SELECT 'two typed paths', toTypeName(c.a.b.c), c.a.b.c, toTypeName(getSubcolumn(c, 'a.b.c')), getSubcolumn(c, 'a.b.c') FROM t_two_typed;
DROP TABLE t_two_typed;

-- A typed path against the generated sizes of a later element.
DROP TABLE IF EXISTS t_typed_sizes;
CREATE TABLE t_typed_sizes (c Tuple(`a` JSON(`b.size0` UInt8), `a.b` Array(UInt64))) ENGINE = Memory;
INSERT INTO t_typed_sizes VALUES (('{"b":{"size0":5}}', [10, 20]));
SELECT 'typed path and sizes', toTypeName(c.a.b.size0), c.a.b.size0, toTypeName(getSubcolumn(c, 'a.b.size0')), getSubcolumn(c, 'a.b.size0') FROM t_typed_sizes;
DROP TABLE t_typed_sizes;

-- The sizes of a typed path against the sizes of a later element: both generated, the first is kept.
DROP TABLE IF EXISTS t_two_sizes;
CREATE TABLE t_two_sizes (c Tuple(`a` JSON(`b` Array(UInt8)), `a.b` Array(UInt64))) ENGINE = Memory;
INSERT INTO t_two_sizes VALUES (('{"b":[1,2,3]}', [10]));
SELECT 'two sizes', toTypeName(c.a.b.size0), c.a.b.size0, toTypeName(getSubcolumn(c, 'a.b.size0')), getSubcolumn(c, 'a.b.size0') FROM t_two_sizes;
DROP TABLE t_two_sizes;

-- A path inside a typed JSON path comes from the data, so it is not a name the type knows and the later
-- element's sizes keep the name.
DROP TABLE IF EXISTS t_path_in_typed;
CREATE TABLE t_path_in_typed (c Tuple(`e` JSON(`p` JSON), `e.p` Array(UInt8))) ENGINE = Memory;
INSERT INTO t_path_in_typed VALUES (('{"p":{"size0":42}}', [1, 2]));
SELECT 'path in a typed path', toTypeName(c.e.p.size0), c.e.p.size0, toTypeName(getSubcolumn(c, 'e.p.size0')), getSubcolumn(c, 'e.p.size0') FROM t_path_in_typed;
DROP TABLE t_path_in_typed;

-- In a single JSON, a typed path below a shorter typed prefix wins over a path the longer typed prefix reads from the data.
DROP TABLE IF EXISTS t_nested_typed;
CREATE TABLE t_nested_typed (j JSON(`a` JSON(`b.c` UInt8), `a.b` JSON)) ENGINE = Memory;
INSERT INTO t_nested_typed VALUES ('{}');
SELECT 'typed path below a prefix', toTypeName(j.a.b.c), j.a.b.c, toTypeName(getSubcolumn(j, 'a.b.c')), getSubcolumn(j, 'a.b.c') FROM t_nested_typed;
DROP TABLE t_nested_typed;

SELECT '--- inside a Variant or a Dynamic, a name the variant type claims is hidden, as for the type itself ---';

SELECT 'Array(JSON)', toTypeName(getSubcolumn(x, 'size0')), toString(getSubcolumn(x, 'size0'))
FROM (SELECT CAST([('{"size0":9}')], 'Array(JSON)') AS x);
SELECT 'Variant(Array(JSON))', toTypeName(getSubcolumn(x, 'Array(JSON).size0')), toString(getSubcolumn(x, 'Array(JSON).size0'))
FROM (SELECT CAST(CAST([('{"size0":9}')], 'Array(JSON)'), 'Variant(Array(JSON))') AS x);
SELECT 'Dynamic of Array(JSON)', toTypeName(getSubcolumn(x, 'Array(JSON).size0')), toString(getSubcolumn(x, 'Array(JSON).size0'))
FROM (SELECT CAST(CAST([('{"size0":9}')], 'Array(JSON)'), 'Dynamic') AS x);

SELECT 'Nullable(JSON)', toTypeName(getSubcolumn(x, 'null')), toString(getSubcolumn(x, 'null'))
FROM (SELECT CAST('{"null":5}', 'Nullable(JSON)') AS x);
SELECT 'Variant(JSON)', toTypeName(getSubcolumn(x, 'JSON.null')), toString(getSubcolumn(x, 'JSON.null'))
FROM (SELECT CAST(CAST('{"null":5}', 'JSON'), 'Variant(JSON)') AS x);
SELECT 'Dynamic of JSON', toTypeName(getSubcolumn(x, 'JSON.null')), toString(getSubcolumn(x, 'JSON.null'))
FROM (SELECT CAST(CAST('{"null":5}', 'JSON'), 'Dynamic') AS x);
SELECT 'Dynamic of Array(JSON), null', toTypeName(getSubcolumn(x, 'Array(JSON).null')), toString(getSubcolumn(x, 'Array(JSON).null'))
FROM (SELECT CAST(CAST([('{"null":5}')], 'Array(JSON)'), 'Dynamic') AS x);

-- A variant type that cannot claim the name keeps the generated subcolumn.
SELECT 'Variant(String)', toTypeName(getSubcolumn(x, 'String.null')), getSubcolumn(x, 'String.null')
FROM (SELECT CAST('a', 'Variant(String)') AS x);
SELECT 'Variant(Array(UInt8))', toTypeName(getSubcolumn(x, 'Array(UInt8).size0')), getSubcolumn(x, 'Array(UInt8).size0')
FROM (SELECT CAST([1, 2], 'Variant(Array(UInt8))') AS x);
SELECT 'Dynamic of String', toTypeName(getSubcolumn(x, 'String.null')), getSubcolumn(x, 'String.null')
FROM (SELECT CAST('a', 'Dynamic') AS x);

-- The listing does not offer the hidden names, and a direct read takes the JSON path.
DROP TABLE IF EXISTS t_variant_json;
CREATE TABLE t_variant_json (v Variant(Array(JSON), JSON)) ENGINE = Memory;
INSERT INTO t_variant_json VALUES (CAST([('{"size0":9}')], 'Array(JSON)'));
DESCRIBE t_variant_json SETTINGS describe_include_subcolumns = 1;
SELECT 'direct read', toTypeName(v.`Array(JSON)`.size0), toString(v.`Array(JSON)`.size0) FROM t_variant_json;
DROP TABLE t_variant_json;

SELECT '--- a value read whole from shared data resolves the name at the array level it was requested at ---';

-- Under the outer array `size1` is the sizes of `p` and `size0` is the declared element; resolved without that
-- level, `size1` would turn into `size0` and read the element.
DROP TABLE IF EXISTS t_shared_level;
CREATE TABLE t_shared_level (x Array(JSON(SHARED p Array(Tuple(size0 UInt8)))))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_shared_level VALUES (['{"p":[{"size0":7},{"size0":8}]}']);
SELECT 'shared typed path, sizes', toTypeName(x.p.size1), x.p.size1 FROM t_shared_level;
SELECT 'shared typed path, element', toTypeName(x.p.size0), x.p.size0 FROM t_shared_level;
DROP TABLE t_shared_level;

-- The same through the shared variant of a Dynamic: with one type allowed, the rarer one goes there on a merge.
DROP TABLE IF EXISTS t_shared_variant_level;
CREATE TABLE t_shared_variant_level (x Array(Dynamic(max_types=1)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_shared_variant_level SELECT [CAST(number, 'Dynamic')] FROM numbers(3);
INSERT INTO t_shared_variant_level SELECT [CAST(CAST([tuple(7), tuple(8)], 'Array(Tuple(size0 UInt8))'), 'Dynamic')];
OPTIMIZE TABLE t_shared_variant_level FINAL;
SELECT 'fixture check: in the shared variant', arrayMap(d -> isDynamicElementInSharedData(d), x) FROM t_shared_variant_level WHERE dynamicType(x[1]) = 'Array(Tuple(size0 UInt8))';
SELECT 'shared variant, sizes', toTypeName(x.`Array(Tuple(size0 UInt8))`.size1), x.`Array(Tuple(size0 UInt8))`.size1
FROM t_shared_variant_level WHERE dynamicType(x[1]) = 'Array(Tuple(size0 UInt8))';
SELECT 'shared variant, element', toTypeName(x.`Array(Tuple(size0 UInt8))`.size0), x.`Array(Tuple(size0 UInt8))`.size0
FROM t_shared_variant_level WHERE dynamicType(x[1]) = 'Array(Tuple(size0 UInt8))';
DROP TABLE t_shared_variant_level;
