-- Regression test: the path-local `JSON` read compatibility of `Dynamic` subcolumns must also recurse
-- through `Map`, so a request for `Map(String, JSON)` returns the rows stored as
-- `Map(String, JSON(a UInt64))` as well.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test_map_json_compat;

CREATE TABLE test_map_json_compat
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

INSERT INTO test_map_json_compat VALUES (1, CAST(map('k', CAST('{"a":1}' AS JSON)) AS Dynamic));
INSERT INTO test_map_json_compat VALUES (2, CAST(CAST(map('k', '{"a":2}') AS Map(String, JSON(a UInt64))) AS Dynamic));
INSERT INTO test_map_json_compat VALUES (3, CAST(42 AS Dynamic));

SELECT '-- separate parts';
SELECT id, d.`Map(String, JSON)`
FROM test_map_json_compat
ORDER BY id
FORMAT TSVRaw;

SELECT '-- single part';
OPTIMIZE TABLE test_map_json_compat FINAL;
SELECT id, d.`Map(String, JSON)`
FROM test_map_json_compat
ORDER BY id
FORMAT TSVRaw;

SELECT '-- dynamicElement';
SELECT id, dynamicElement(d, 'Map(String, JSON)')
FROM test_map_json_compat
ORDER BY id
FORMAT TSVRaw;

DROP TABLE test_map_json_compat;

SELECT '-- shared variant';
DROP TABLE IF EXISTS test_map_json_compat_shared;

-- With `Dynamic(max_types=0)` every value is stored in the shared variant.
CREATE TABLE test_map_json_compat_shared
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

INSERT INTO test_map_json_compat_shared VALUES
    (1, CAST(map('k', CAST('{"a":1}' AS JSON)) AS Dynamic)),
    (2, CAST(CAST(map('k', '{"a":2}') AS Map(String, JSON(a UInt64))) AS Dynamic)),
    (3, CAST(42 AS Dynamic));

SELECT id, d.`Map(String, JSON)`
FROM test_map_json_compat_shared
ORDER BY id
FORMAT TSVRaw;

DROP TABLE test_map_json_compat_shared;
