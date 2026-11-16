-- Regression test: reading a `Dynamic` subcolumn of a declared-path `JSON` type must not treat rows stored
-- with a `JSON` type that lacks one of the requested typed paths as matching: converting such a value to the
-- requested type reparses the path (`{"a":"x"}` fails, `{"a":"1"}` is coerced to `1`), while the `Dynamic`
-- element contract is that rows of another type read as absent. Rows whose stored type declares every
-- requested typed path (and possibly more) stay visible, for whole-type and nested subcolumn reads alike.

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

SELECT '-- in memory';
SELECT dynamicType(d), dynamicElement(d, 'JSON(a UInt64)'), d.`JSON(a UInt64)`, d.`JSON(a UInt64)`.a, d.JSON.a, d.JSON
FROM (SELECT arrayJoin([
    CAST(CAST('{"a":"x"}', 'JSON'), 'Dynamic'),
    CAST(CAST('{"a":"1"}', 'JSON'), 'Dynamic'),
    CAST(CAST('{"a":5,"b":"q"}', 'JSON(a UInt64, b String)'), 'Dynamic'),
    CAST(CAST('{"a":7}', 'JSON(a UInt64)'), 'Dynamic'),
    CAST(CAST('{"a":8}', 'JSON(a String)'), 'Dynamic'),
    CAST(42, 'Dynamic')]) AS d)
FORMAT TSV;

-- Pin serialization versions: randomized `object_*_serialization_version` settings change how the stored
-- type of a `JSON` value is rendered and split across streams.
DROP TABLE IF EXISTS t_dyn_json_declared;
CREATE TABLE t_dyn_json_declared (id UInt64, d Dynamic(max_types=8)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_json_declared VALUES
    (1, CAST('{"a":"x"}', 'JSON')),
    (2, CAST('{"a":"1"}', 'JSON')),
    (3, CAST('{"a":5,"b":"q"}', 'JSON(a UInt64, b String)')),
    (4, CAST('{"a":7}', 'JSON(a UInt64)')),
    (5, CAST('{"a":8}', 'JSON(a String)')),
    (6, 42);

SELECT '-- named variants';
SELECT id, dynamicType(d), dynamicElement(d, 'JSON(a UInt64)'), d.`JSON(a UInt64)`, d.`JSON(a UInt64)`.a, d.JSON.a, d.JSON
FROM t_dyn_json_declared ORDER BY id FORMAT TSV;
SELECT id, d.`JSON(a UInt64)`.null, d.`JSON(a UInt64, b String)`.a FROM t_dyn_json_declared ORDER BY id;

DROP TABLE t_dyn_json_declared;

-- With max_types=0 every value lives in the shared variant.
SELECT '-- shared variant';
CREATE TABLE t_dyn_json_declared_shared (id UInt64, d Dynamic(max_types=0)) ENGINE = MergeTree ORDER BY id
SETTINGS object_serialization_version = 'v1', object_shared_data_serialization_version = 'map',
         object_shared_data_serialization_version_for_zero_level_parts = 'map', dynamic_serialization_version = 'v2',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_json_declared_shared VALUES
    (1, CAST('{"a":"x"}', 'JSON')),
    (2, CAST('{"a":"1"}', 'JSON')),
    (3, CAST('{"a":5,"b":"q"}', 'JSON(a UInt64, b String)')),
    (4, CAST('{"a":7}', 'JSON(a UInt64)')),
    (5, CAST('{"a":8}', 'JSON(a String)')),
    (6, 42);

SELECT id, dynamicType(d), dynamicElement(d, 'JSON(a UInt64)'), d.`JSON(a UInt64)`, d.`JSON(a UInt64)`.a, d.JSON.a, d.JSON
FROM t_dyn_json_declared_shared ORDER BY id FORMAT TSV;
SELECT id, d.`JSON(a UInt64)`.null, d.`JSON(a UInt64, b String)`.a FROM t_dyn_json_declared_shared ORDER BY id;

DROP TABLE t_dyn_json_declared_shared;
