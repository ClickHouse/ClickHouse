SET enable_json_type = 1;
SET allow_experimental_json_type = 1;

-- A typed path with a NULL literal but a non-empty sub-object must still be reported as
-- present by JSONHas / JSONExtractRaw when type_json_skip_null_typed_paths is enabled.
DROP TABLE IF EXISTS t_json_null_typed_subobject;
CREATE TABLE t_json_null_typed_subobject (id UInt8, json JSON(a Nullable(Int64))) ENGINE = Memory;
INSERT INTO t_json_null_typed_subobject VALUES (1, '{"a.b": 42}'), (2, '{"a": 1}'), (3, '{}');

SELECT 'without setting';
SELECT id, JSONAllPaths(json), has(json, 'a'), JSONHas(json, 'a'), JSONExtractRaw(json, 'a')
FROM t_json_null_typed_subobject ORDER BY id;

SELECT 'with setting';
SELECT id, json, JSONAllPaths(json), has(json, 'a'), JSONHas(json, 'a'), JSONExtractRaw(json, 'a')
FROM t_json_null_typed_subobject ORDER BY id
SETTINGS type_json_skip_null_typed_paths = 1;

-- Same data with dynamic paths spilled to shared data.
DROP TABLE IF EXISTS t_json_null_typed_subobject_shared;
CREATE TABLE t_json_null_typed_subobject_shared (id UInt8, json JSON(a Nullable(Int64), max_dynamic_paths=0)) ENGINE = Memory;
INSERT INTO t_json_null_typed_subobject_shared VALUES (1, '{"a.b": 42}'), (2, '{"a": 1}'), (3, '{}');

SELECT 'shared data with setting';
SELECT id, json, JSONAllPaths(json), has(json, 'a'), JSONHas(json, 'a'), JSONExtractRaw(json, 'a')
FROM t_json_null_typed_subobject_shared ORDER BY id
SETTINGS type_json_skip_null_typed_paths = 1;

-- Non-nullable typed path is unaffected: 0 and missing keys stay present.
DROP TABLE IF EXISTS t_json_non_nullable;
CREATE TABLE t_json_non_nullable (json JSON(a Int64)) ENGINE = Memory;
INSERT INTO t_json_non_nullable VALUES ('{"a": 0}'), ('{}');

SELECT 'non-nullable with setting';
SELECT json, JSONHas(json, 'a'), JSONExtractRaw(json, 'a') FROM t_json_non_nullable ORDER BY rowNumberInAllBlocks()
SETTINGS type_json_skip_null_typed_paths = 1;

-- Both a and a.b are typed. A NULL parent with a non-NULL child is present; both NULL is absent.
DROP TABLE IF EXISTS t_json_both_typed_null;
CREATE TABLE t_json_both_typed_null (json JSON(a Nullable(Int64), a.b Nullable(Int64))) ENGINE = Memory;
INSERT INTO t_json_both_typed_null VALUES ('{"a": 1}'), ('{"a.b": 42}'), ('{"a": null, "a.b": null}'), ('{}');

SELECT 'both typed JSONHas with setting';
SELECT JSONHas(json, 'a'), JSONHas(json, 'a.b') FROM t_json_both_typed_null ORDER BY rowNumberInAllBlocks()
SETTINGS type_json_skip_null_typed_paths = 1;

SELECT 'both typed JSONExtractRaw with setting';
SELECT JSONExtractRaw(json, 'a'), JSONExtractRaw(json, 'a.b') FROM t_json_both_typed_null ORDER BY rowNumberInAllBlocks()
SETTINGS type_json_skip_null_typed_paths = 1;

SELECT 'both typed has with setting';
SELECT has(json, 'a'), has(json, 'a.b') FROM t_json_both_typed_null ORDER BY rowNumberInAllBlocks()
SETTINGS type_json_skip_null_typed_paths = 1;

DROP TABLE t_json_null_typed_subobject;
DROP TABLE t_json_null_typed_subobject_shared;
DROP TABLE t_json_non_nullable;
DROP TABLE t_json_both_typed_null;
