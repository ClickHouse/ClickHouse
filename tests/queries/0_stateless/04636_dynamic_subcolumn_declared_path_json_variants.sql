-- Regression test: `Dynamic` nested subcolumn reads must be path-local compatible for `JSON`:
-- a request such as `d.JSON.a` must also return rows stored under declared-path variants such as
-- `JSON(a UInt64)` or `JSON(a UInt64, b String)`, not only rows with settings-compatible variants.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test_declared_path_compat;

CREATE TABLE test_declared_path_compat
(
    id UInt64,
    d Dynamic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    dynamic_serialization_version = 'v3';

INSERT INTO test_declared_path_compat VALUES (1, CAST(CAST('{"a":1}' AS JSON) AS Dynamic));
INSERT INTO test_declared_path_compat VALUES (2, CAST(CAST('{"a":2}' AS JSON(a UInt64)) AS Dynamic));
INSERT INTO test_declared_path_compat VALUES (3, CAST(CAST('{"a":3,"b":"x"}' AS JSON(a UInt64, b String)) AS Dynamic));
INSERT INTO test_declared_path_compat VALUES (4, CAST(CAST('{"b":"y"}' AS JSON(b String)) AS Dynamic));
INSERT INTO test_declared_path_compat VALUES (5, CAST(42 AS Dynamic));
INSERT INTO test_declared_path_compat VALUES (6, CAST(CAST('{"a":"x"}' AS JSON(a String)) AS Dynamic));

SELECT '-- separate parts';
SELECT id, dynamicType(d), d.JSON.a, d.JSON.a.:Int64, d.JSON.b
FROM test_declared_path_compat
ORDER BY id
FORMAT TSVRaw;

SELECT '-- single part';
OPTIMIZE TABLE test_declared_path_compat FINAL;
SELECT id, dynamicType(d), d.JSON.a, d.JSON.a.:Int64, d.JSON.b
FROM test_declared_path_compat
ORDER BY id
FORMAT TSVRaw;

-- An incompatible type on a shared declared path is not read as a compatible `JSON` value.
SELECT id, dynamicElement(d, 'JSON(a UInt64)')
FROM test_declared_path_compat
WHERE id = 6
FORMAT TSVRaw;

DROP TABLE test_declared_path_compat;

SELECT '-- shared variant';
DROP TABLE IF EXISTS test_declared_path_compat_shared;

-- With `Dynamic(max_types=0)` every value is stored in the shared variant.
CREATE TABLE test_declared_path_compat_shared
(
    id UInt64,
    d Dynamic(max_types=0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    dynamic_serialization_version = 'v3';

INSERT INTO test_declared_path_compat_shared VALUES (1, CAST(CAST('{"a":1}' AS JSON) AS Dynamic)), (2, CAST(CAST('{"a":2}' AS JSON(a UInt64)) AS Dynamic)), (3, CAST(42 AS Dynamic));

SELECT id, dynamicType(d), d.JSON.a, d.JSON.a.:Int64
FROM test_declared_path_compat_shared
ORDER BY id
FORMAT TSVRaw;

DROP TABLE test_declared_path_compat_shared;

-- The in-memory path (`DataTypeDynamic::getDynamicSubcolumnData`) is covered by
-- `04651_dynamic_subcolumn_declared_path_json_in_memory` (it needs the analyzer).
