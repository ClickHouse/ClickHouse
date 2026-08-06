-- Tags: no-fasttest
-- no-fasttest: the JSON type is not supported in the fast test build.

-- A "sizeN" subcolumn name is only meaningful together with the array nesting level it was
-- resolved at. Deserialization used to re-resolve such a name at level 0, so resolution and
-- deserialization disagreed on the type and insertRangeFrom aborted.

DROP TABLE IF EXISTS t04812_json;
CREATE TABLE t04812_json (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_json FORMAT JSONAsObject {"a" : [{"b" : [42, 43]}]}
;

SELECT '-- expression depths: sizeN with N = depth - 1 is the array-sizes subcolumn';
SELECT toTypeName(json.a.:`Array(JSON)`), toTypeName(json.a.:`Array(JSON)`.b.:`Array(JSON)`) FROM t04812_json;

SELECT '-- WITNESS depth 2, at level';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.size1 FROM t04812_json;

SELECT '-- WITNESS depth 2, below level: aborted too; the value is a default because the name resolves as an ordinary JSON path';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.size0 FROM t04812_json;

SELECT '-- CONTROL depth 2, above level: a non-existent path, returns a default';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.size2, json.a.:`Array(JSON)`.b.:`Array(JSON)`.size3 FROM t04812_json;

SELECT '-- CONTROL a non-reserved name is unaffected';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.zzz FROM t04812_json;

SELECT '-- CONTROL depth 1: the two levels already agreed, must be untouched';
SELECT json.a.:`Array(JSON)`.size0, json.a.:`Array(JSON)`.size1 FROM t04812_json;

SELECT '-- WITNESS depth 2 through an expression that also produces a constant column';
SELECT arrayJoin([1]) AS x, json.a.:`Array(JSON)`.b.:`Array(JSON)`.size1 FROM t04812_json;

DROP TABLE t04812_json;

DROP TABLE IF EXISTS t04812_json3;
CREATE TABLE t04812_json3 (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_json3 FORMAT JSONAsObject {"a" : [{"b" : [{"c" : [42, 43]}]}]}
;

SELECT '-- WITNESS depth 3, at level';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.c.:`Array(JSON)`.size2 FROM t04812_json3;

SELECT '-- WITNESS depth 3, below level: size0 disagreed, size1 happened to agree';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.c.:`Array(JSON)`.size0, json.a.:`Array(JSON)`.b.:`Array(JSON)`.c.:`Array(JSON)`.size1 FROM t04812_json3;

SELECT '-- CONTROL depth 3, above level';
SELECT json.a.:`Array(JSON)`.b.:`Array(JSON)`.c.:`Array(JSON)`.size3 FROM t04812_json3;

DROP TABLE t04812_json3;

-- The arms above request Array(JSON) against a path holding Array(Int64), so the shared-variant
-- type-name comparison never matches and their values are defaults. Here the requested type is
-- really present in the shared variant (max_dynamic_types=1 with an Int64 majority evicts it
-- there), so the extracted sizes are non-zero and an implementation that always defaulted the
-- extraction would not reproduce them.
DROP TABLE IF EXISTS t04812_evicted;
CREATE TABLE t04812_evicted (jd Array(JSON(max_dynamic_types=1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_evicted FORMAT JSONEachRow {"jd":[{"a":1}]} {"jd":[{"a":2}]} {"jd":[{"a":3}]} {"jd":[{"a":[{"k":1},{"k":2}]}]}
;

SELECT '-- fixture check: the requested type must sit in the shared variant';
SELECT arrayMap(x -> isDynamicElementInSharedData(x), jd.a) FROM t04812_evicted;

SELECT '-- WITNESS shared variant, at level: the extracted size is non-zero';
SELECT jd.a.:`Array(JSON)`.size1 FROM t04812_evicted;

SELECT '-- CONTROL shared variant, above level';
SELECT jd.a.:`Array(JSON)`.size2 FROM t04812_evicted;

DROP TABLE t04812_evicted;

SELECT '-- CONTROL plain nested arrays: sizeN is root-relative and was never broken';
DROP TABLE IF EXISTS t04812_arr;
CREATE TABLE t04812_arr (arr Array(Array(Array(UInt64)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_arr VALUES ([[[1, 2], [3]], [[4]]]);
SELECT toTypeName(arr.size0), toTypeName(arr.size1), toTypeName(arr.size2) FROM t04812_arr;
SELECT arr.size0, arr.size1, arr.size2 FROM t04812_arr;
DROP TABLE t04812_arr;

-- The shared data version is pinned rather than left to the runner: the "advanced" version
-- reaches a different, pre-existing stream-enumeration defect that is out of scope here, and a
-- DDL setting wins over the runner injection (ClientBase::addMergeTreeSettings).
DROP TABLE IF EXISTS t04812_shared;
CREATE TABLE t04812_shared (jd Array(JSON(max_dynamic_paths=1))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map';
INSERT INTO t04812_shared FORMAT JSONEachRow {"jd":[{"zz":1,"yy":2,"a":[{"b":[42,43]}]}]}
;

SELECT '-- fixture check: the path must be in shared data and the parent must be non-empty';
SELECT JSONSharedDataPaths(jd[1]), length(jd.a) FROM t04812_shared;

SELECT '-- WITNESS shared data, at level';
SELECT jd.a.:`Array(JSON)`.size1 FROM t04812_shared;

SELECT '-- WITNESS shared data, below level';
SELECT jd.a.:`Array(JSON)`.size0 FROM t04812_shared;

SELECT '-- CONTROL shared data, above level';
SELECT jd.a.:`Array(JSON)`.size2 FROM t04812_shared;

DROP TABLE t04812_shared;

DROP TABLE IF EXISTS t04812_map;
CREATE TABLE t04812_map (mp Array(Array(Map(String, JSON)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_map FORMAT JSONEachRow {"mp":[[{"k":{"a":[{"b":[42,43]}]}}]]}
;

SELECT '-- WITNESS a Map between the array wrappers and the dynamic path';
SELECT toTypeName(mp.values.values.a.:`Array(JSON)`) FROM t04812_map;
SELECT mp.values.values.a.:`Array(JSON)`.size3, mp.values.values.a.:`Array(JSON)`.size0 FROM t04812_map;

SELECT '-- CONTROL Map, above level';
SELECT mp.values.values.a.:`Array(JSON)`.size2 FROM t04812_map;

DROP TABLE t04812_map;

DROP TABLE IF EXISTS t04812_shared_adv;
CREATE TABLE t04812_shared_adv (jd Array(JSON(max_dynamic_paths=1))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
INSERT INTO t04812_shared_adv FORMAT JSONEachRow {"jd":[{"zz":1,"yy":2,"a":[{"b":[42,43]}]}]}
;

-- The whole path is selected alongside the subcolumn on purpose: requesting the subcolumn on
-- its own under the advanced version reaches a separate, pre-existing enumerateStreams defect.
SELECT '-- WITNESS advanced shared data: the whole path plus the subcolumn';
SELECT jd.a, jd.a.:`Array(JSON)`.size1 FROM t04812_shared_adv;

DROP TABLE t04812_shared_adv;
