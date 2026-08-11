-- Whole-type `Dynamic` subcolumn reads must also look through a nested `Dynamic` carrier: two
-- container types differing only in the `max_dynamic_types` of a nested `Dynamic` are convertible
-- to each other, so a request for `Array(Dynamic(max_types=5))` must see rows stored as
-- `Array(Dynamic(max_types=2))`. This exercises both the in-memory path
-- (`DataTypeDynamic::getDynamicSubcolumnData`) and the storage path (`SerializationDynamicElement`).

SET enable_json_type = 1;

-- Pin serialization versions: the randomized `object_*`/`dynamic_serialization_version` settings
-- change how a stored type is rendered and split across streams.
DROP TABLE IF EXISTS t_dyn_carrier;
CREATE TABLE t_dyn_carrier (id UInt64, d Dynamic(max_types=4)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_carrier VALUES (1, [1::Int64]::Array(Dynamic(max_types=2))), (2, ['x']::Array(Dynamic(max_types=5))), (3, 42::Int64);

SELECT 'named variants';
SELECT id, dynamicType(d) FROM t_dyn_carrier ORDER BY id;
SELECT id, d.`Array(Dynamic(max_types=5))` FROM t_dyn_carrier ORDER BY id;
SELECT id, dynamicElement(d, 'Array(Dynamic(max_types=5))') FROM t_dyn_carrier ORDER BY id;
SELECT id, d.`Array(Dynamic(max_types=2))` FROM t_dyn_carrier ORDER BY id;

DROP TABLE t_dyn_carrier;

-- With `max_types=0` every value lives in the shared variant, exercising the shared-variant branch.
SELECT 'shared variant';
CREATE TABLE t_dyn_carrier_shared (id UInt64, d Dynamic(max_types=0)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_carrier_shared VALUES (1, [1::Int64]::Array(Dynamic(max_types=2))), (2, ['x']::Array(Dynamic(max_types=5))), (3, 42::Int64);

SELECT id, dynamicType(d) FROM t_dyn_carrier_shared ORDER BY id;
SELECT id, d.`Array(Dynamic(max_types=5))` FROM t_dyn_carrier_shared ORDER BY id;

DROP TABLE t_dyn_carrier_shared;

-- A nested `Variant` carrier stays incompatible: `CAST` between two `Variant` types is only allowed
-- when the target extends the source, so there is no conversion between `Variant(JSON(a UInt64),
-- Int64)` and `Variant(JSON, Int64)` in either direction and such rows must keep reading as absent
-- rather than raising `CANNOT_CONVERT_TYPE`.
SELECT 'nested variant carrier stays absent';
CREATE TABLE t_var_carrier (id UInt64, d Dynamic(max_types=4)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_var_carrier
    SETTINGS allow_suspicious_variant_types = 1
    SELECT 1, CAST([CAST('{"a":1}'::JSON(a UInt64) AS Variant(JSON(a UInt64), Int64))] AS Dynamic);
INSERT INTO t_var_carrier
    SETTINGS allow_suspicious_variant_types = 1
    SELECT 2, CAST([CAST(42::Int64 AS Variant(JSON, Int64))] AS Dynamic);

SELECT id, dynamicType(d) FROM t_var_carrier ORDER BY id;
SELECT id, d.`Array(Variant(JSON, Int64))` FROM t_var_carrier ORDER BY id;

DROP TABLE t_var_carrier;
