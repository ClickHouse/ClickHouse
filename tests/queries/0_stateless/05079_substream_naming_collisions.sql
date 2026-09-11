-- Types whose streams collide in one file name under the old scheme. The namespaces make the names
-- injective, so each of these has to round-trip under 'namespaced'.

-- Array sizes against a tuple element that claims their name. The files already differ under the old
-- scheme; the subcolumn name is ambiguous under both, and resolves to the element.
DROP TABLE IF EXISTS t_size0;
CREATE TABLE t_size0 (c Array(Tuple(`size0` UInt64, v String))) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_size0 VALUES ([(7,'ab'),(8,'cde')]);
SELECT 'array sizes against a tuple element named size0';
SELECT c, length(c) FROM t_size0;
DROP TABLE t_size0;

-- A tuple element named after a nested path: `a.b` and `a` + `b` render to one file under the old
-- scheme, which loses one of the two streams.
DROP TABLE IF EXISTS t_dotted;
CREATE TABLE t_dotted (c Tuple(`a` Tuple(`b` UInt64), `a.b` UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_dotted VALUES (((1), 2));
SELECT 'tuple element named a.b beside a nested a.b';
SELECT c FROM t_dotted;
SELECT tupleElement(c, 'a.b'), tupleElement(tupleElement(c, 'a'), 'b') FROM t_dotted;
SELECT countDistinct(f) FROM (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_dotted' AND active);
DROP TABLE t_dotted;

-- There is no JSON counterpart of the case above: a dotted path name there *is* a nested path, so
-- `JSON(a Tuple(b Int64), `a.b` Int64)` declares two paths that no document can populate at once.

-- A declared path colliding with an automatic stream of the enclosing type.
DROP TABLE IF EXISTS t_json_structure;
CREATE TABLE t_json_structure (c JSON(`object_structure` Int64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_json_structure VALUES ('{"object_structure":5}');
SELECT 'json path named object_structure';
SELECT c.object_structure FROM t_json_structure;
DROP TABLE t_json_structure;

-- A null map against a JSON path of the same name.
DROP TABLE IF EXISTS t_json_null;
CREATE TABLE t_json_null (c Nullable(JSON(`null` Int64))) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_json_null VALUES ('{"null":5}'), (NULL);
SELECT 'json path named null inside Nullable';
SELECT c FROM t_json_null ORDER BY isNull(c);
DROP TABLE t_json_null;

-- Subcolumns extracted from a Nullable that cannot hold NULL themselves (Tuple, Map, and the Arrays
-- inside the Map) only resolve to the right files if the read reproduces the Nullable path element.
DROP TABLE IF EXISTS t_nullable_json;
CREATE TABLE t_nullable_json (c Nullable(JSON(`a` Tuple(b Int64), `m` Map(String, Int64))))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_nullable_json VALUES ('{"a":{"b":1},"m":{"k":2}}');
SELECT 'subcolumns extracted from Nullable';
SELECT c.a, c.a.b FROM t_nullable_json;
SELECT c.m, c.m.size0, c.m.keys, c.m.values FROM t_nullable_json;
SELECT c.m.keys.size FROM t_nullable_json;
DROP TABLE t_nullable_json;

-- Sparse serialization takes part in the cache key, so it has to keep its own entries apart from the
-- dense ones for the same paths.
DROP TABLE IF EXISTS t_sparse;
CREATE TABLE t_sparse (id UInt64, a Array(UInt64), s String) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             ratio_of_defaults_for_sparse_serialization = 0.5, substream_naming_version = 'namespaced';
INSERT INTO t_sparse SELECT number, if(number = 5, [1,2], []), if(number = 5, 'ab', '') FROM numbers(20);
SELECT 'sparse';
SELECT sum(a.size0), countIf(s != ''), sum(length(a)) FROM t_sparse;
SELECT a, s FROM t_sparse WHERE id = 5;
DROP TABLE t_sparse;
