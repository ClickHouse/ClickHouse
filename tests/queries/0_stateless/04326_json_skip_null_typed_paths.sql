SET enable_json_type = 1;
SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS test_json_skip_null;
CREATE TABLE test_json_skip_null (json JSON(a Nullable(Int64), b Nullable(String))) ENGINE = Memory;
INSERT INTO test_json_skip_null VALUES ('{"a": 42, "b": "hello"}'), ('{"a": null, "b": null}'), ('{"a": null, "b": "world"}'), ('{}');

-- Without the setting (default): typed paths are always present.
SELECT 'serialization without setting';
SELECT json FROM test_json_skip_null ORDER BY rowNumberInAllBlocks();

SELECT 'serialization with setting';
SELECT json FROM test_json_skip_null ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

-- JSONAllPaths
SELECT 'JSONAllPaths without setting';
SELECT JSONAllPaths(json) FROM test_json_skip_null ORDER BY rowNumberInAllBlocks();

SELECT 'JSONAllPaths with setting';
SELECT JSONAllPaths(json) FROM test_json_skip_null ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

-- JSONAllPathsWithTypes
SELECT 'JSONAllPathsWithTypes without setting';
SELECT JSONAllPathsWithTypes(json) FROM test_json_skip_null ORDER BY rowNumberInAllBlocks();

SELECT 'JSONAllPathsWithTypes with setting';
SELECT JSONAllPathsWithTypes(json) FROM test_json_skip_null ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

-- empty / notEmpty
SELECT 'empty without setting';
SELECT empty(json) FROM test_json_skip_null ORDER BY rowNumberInAllBlocks();

SELECT 'empty with setting';
SELECT empty(json) FROM test_json_skip_null ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

-- JSONHas
SELECT 'JSONHas without setting';
SELECT JSONHas(json, 'a'), JSONHas(json, 'b') FROM test_json_skip_null ORDER BY rowNumberInAllBlocks();

SELECT 'JSONHas with setting';
SELECT JSONHas(json, 'a'), JSONHas(json, 'b') FROM test_json_skip_null ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

-- Test with non-nullable typed path: setting should NOT affect it.
DROP TABLE IF EXISTS test_json_non_nullable;
CREATE TABLE test_json_non_nullable (json JSON(a Int64)) ENGINE = Memory;
INSERT INTO test_json_non_nullable VALUES ('{"a": 0}'), ('{}');

SELECT 'non-nullable typed path with setting';
SELECT json FROM test_json_non_nullable ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

SELECT 'non-nullable JSONHas with setting';
SELECT JSONHas(json, 'a') FROM test_json_non_nullable ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

-- Test mixed typed and dynamic paths.
DROP TABLE IF EXISTS test_json_mixed;
CREATE TABLE test_json_mixed (json JSON(a Nullable(Int64))) ENGINE = Memory;
INSERT INTO test_json_mixed VALUES ('{"a": 42, "d": 100}'), ('{"a": null, "d": 200}'), ('{"a": null}');

SELECT 'mixed typed+dynamic with setting';
SELECT json FROM test_json_mixed ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

SELECT 'mixed JSONAllPaths with setting';
SELECT JSONAllPaths(json) FROM test_json_mixed ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

SELECT 'mixed empty with setting';
SELECT empty(json) FROM test_json_mixed ORDER BY rowNumberInAllBlocks() SETTINGS type_json_skip_null_typed_paths = 1;

DROP TABLE test_json_skip_null;
DROP TABLE test_json_non_nullable;
DROP TABLE test_json_mixed;
