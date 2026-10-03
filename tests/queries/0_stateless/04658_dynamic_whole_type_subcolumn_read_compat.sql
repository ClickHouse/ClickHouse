-- Whole-type `Dynamic` subcolumn reads must use path-local read compatibility on the storage
-- read path, matching the in-memory path (`DataTypeDynamic::getDynamicSubcolumnData`): rows
-- stored as `JSON(a UInt64)` / `Map(String, JSON(a UInt64))` must be visible when reading
-- `d.JSON` / `d.`Map(String, JSON)``, including values that live in the shared variant.

-- Pin serialization versions: randomized `object_*_serialization_version` settings change how
-- the stored type of a `JSON` value is rendered and split across streams.
DROP TABLE IF EXISTS t_dyn_whole;
CREATE TABLE t_dyn_whole (id UInt64, d Dynamic(max_types=4)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_whole VALUES (1, '{"a":2}'::JSON(a UInt64)), (2, map('k', '{"a":3}'::JSON(a UInt64))::Map(String, JSON(a UInt64))), (3, 42::Int64);

SELECT 'named variants';
SELECT id, dynamicType(d) FROM t_dyn_whole ORDER BY id;
SELECT id, d.`JSON` FROM t_dyn_whole ORDER BY id;
SELECT id, d.`Map(String, JSON)` FROM t_dyn_whole ORDER BY id;
SELECT id, dynamicElement(d, 'Map(String, JSON)') FROM t_dyn_whole ORDER BY id;
SELECT id, d.`JSON`.null FROM t_dyn_whole ORDER BY id;

DROP TABLE t_dyn_whole;

-- With max_types=0 every value lives in the shared variant, exercising the shared-variant
-- branch of the whole-type read.
SELECT 'shared variant';
CREATE TABLE t_dyn_shared (id UInt64, d Dynamic(max_types=0)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_shared VALUES (1, '{"a":2}'::JSON(a UInt64)), (2, map('k', '{"a":3}'::JSON(a UInt64))::Map(String, JSON(a UInt64))), (3, 42::Int64);

SELECT id, dynamicType(d) FROM t_dyn_shared ORDER BY id;
SELECT id, d.`JSON` FROM t_dyn_shared ORDER BY id;
SELECT id, d.`Map(String, JSON)` FROM t_dyn_shared ORDER BY id;
SELECT id, d.`JSON`.null FROM t_dyn_shared ORDER BY id;

DROP TABLE t_dyn_shared;
