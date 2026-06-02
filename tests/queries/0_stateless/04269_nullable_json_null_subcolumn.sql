set enable_analyzer=1;

-- Baseline: for Nullable(String), .null is still the UInt8 null-map.
SELECT toTypeName(str.null) FROM (SELECT CAST(NULL AS Nullable(String)) AS str);

-- For Nullable(JSON), .null is the Dynamic JSON path, not the UInt8 null-map.
SELECT toTypeName(json.null) FROM (SELECT CAST(NULL AS Nullable(JSON)) AS json);

-- For Nullable(JSON('null' String)) with a typed hint, .null is Nullable(String).
SELECT toTypeName(json.null) FROM (SELECT CAST(NULL AS Nullable(JSON(null String))) AS json);

SELECT '--- Memory engine ---';

DROP TABLE IF EXISTS t_nullable_json_null;
CREATE TABLE t_nullable_json_null (id UInt32, json Nullable(JSON)) ENGINE = Memory;

INSERT INTO t_nullable_json_null VALUES (1, NULL), (2, '{"null": "hello"}'), (3, '{"other": 1}');

-- col.null returns the JSON "null" path value (Dynamic), not the null-map.
SELECT id, json.null, toTypeName(json.null) FROM t_nullable_json_null ORDER BY id;

-- isNull/isNotNull must reflect the outer Nullable null-map, not the JSON "null" path.
SELECT id, isNull(json), isNotNull(json) FROM t_nullable_json_null ORDER BY id;

-- count(col) must count non-null rows only.
SELECT count(json) FROM t_nullable_json_null;

DROP TABLE t_nullable_json_null;

SELECT '--- Memory engine, typed null path ---';

DROP TABLE IF EXISTS t_nullable_json_null_typed;
CREATE TABLE t_nullable_json_null_typed (id UInt32, json Nullable(JSON(null String))) ENGINE = Memory;

INSERT INTO t_nullable_json_null_typed VALUES (1, NULL), (2, '{"null": "hello"}'), (3, '{"other": 1}');

-- col.null is Nullable(String); the outer null-map is applied.
SELECT id, json.null, toTypeName(json.null) FROM t_nullable_json_null_typed ORDER BY id;

SELECT id, isNull(json), isNotNull(json) FROM t_nullable_json_null_typed ORDER BY id;

SELECT count(json) FROM t_nullable_json_null_typed;

DROP TABLE t_nullable_json_null_typed;

SELECT '--- Wide MergeTree ---';

DROP TABLE IF EXISTS t_nullable_json_null_wide;
CREATE TABLE t_nullable_json_null_wide (id UInt32, json Nullable(JSON))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO t_nullable_json_null_wide VALUES (1, NULL), (2, '{"null": "hello"}'), (3, '{"other": 1}');

SELECT id, json.null, toTypeName(json.null) FROM t_nullable_json_null_wide ORDER BY id;
SELECT id, isNull(json), isNotNull(json) FROM t_nullable_json_null_wide ORDER BY id;
SELECT count(json) FROM t_nullable_json_null_wide;

DROP TABLE t_nullable_json_null_wide;

SELECT '--- Compact MergeTree ---';

DROP TABLE IF EXISTS t_nullable_json_null_compact;
CREATE TABLE t_nullable_json_null_compact (id UInt32, json Nullable(JSON))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000;

INSERT INTO t_nullable_json_null_compact VALUES (1, NULL), (2, '{"null": "hello"}'), (3, '{"other": 1}');

SELECT id, json.null, toTypeName(json.null) FROM t_nullable_json_null_compact ORDER BY id;
SELECT id, isNull(json), isNotNull(json) FROM t_nullable_json_null_compact ORDER BY id;
SELECT count(json) FROM t_nullable_json_null_compact;

DROP TABLE t_nullable_json_null_compact;
