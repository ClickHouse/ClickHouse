-- Regression test: reading a `Dynamic` subcolumn of a `JSON` type with `SKIP` / `SKIP REGEXP` rules must not
-- treat rows stored with a `JSON` type that does not skip those paths as matching: converting such a value to
-- the requested type would silently drop the skipped paths, while the `Dynamic` element contract is that rows
-- of another type read as absent. Rows whose stored type skips every requested path (and possibly more) stay
-- visible.

SET enable_json_type = 1;

SELECT '-- in memory';
SELECT dynamicType(d), dynamicElement(d, 'JSON(SKIP b)'), d.`JSON(SKIP b)`, d.`JSON(SKIP REGEXP \'^c\')`, d.JSON
FROM (SELECT arrayJoin([
    CAST(CAST('{"a":1,"b":2,"c":3}', 'JSON'), 'Dynamic'),
    CAST(CAST('{"a":4,"b":5,"c":6}', 'JSON(SKIP b)'), 'Dynamic'),
    CAST(CAST('{"a":7,"b":8,"c":9}', 'JSON(SKIP b, SKIP REGEXP \'^c\')'), 'Dynamic'),
    CAST(42, 'Dynamic')]) AS d)
FORMAT TSV;

SELECT '-- array';
SELECT dynamicType(d), dynamicElement(d, 'Array(JSON(SKIP b))')
FROM (SELECT arrayJoin([
    CAST([CAST('{"a":1,"b":2}', 'JSON')], 'Dynamic'),
    CAST(CAST([CAST('{"a":3,"b":4}', 'JSON')], 'Array(JSON(SKIP b))'), 'Dynamic')]) AS d)
FORMAT TSV;

DROP TABLE IF EXISTS t_dyn_json_skip;
CREATE TABLE t_dyn_json_skip (id UInt64, d Dynamic(max_types=8)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_json_skip VALUES
    (1, CAST('{"a":1,"b":2,"c":3}', 'JSON')),
    (2, CAST('{"a":4,"b":5,"c":6}', 'JSON(SKIP b)')),
    (3, CAST('{"a":7,"b":8,"c":9}', 'JSON(SKIP b, SKIP REGEXP \'^c\')')),
    (4, 42);

SELECT '-- named variants';
SELECT id, dynamicType(d), dynamicElement(d, 'JSON(SKIP b)'), d.`JSON(SKIP b)`, d.`JSON(SKIP REGEXP \'^c\')`
FROM t_dyn_json_skip ORDER BY id FORMAT TSV;

DROP TABLE t_dyn_json_skip;

-- With max_types=0 every value lives in the shared variant.
CREATE TABLE t_dyn_json_skip_shared (id UInt64, d Dynamic(max_types=0)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_dyn_json_skip_shared VALUES
    (1, CAST('{"a":1,"b":2,"c":3}', 'JSON')),
    (2, CAST('{"a":4,"b":5,"c":6}', 'JSON(SKIP b)')),
    (3, CAST('{"a":7,"b":8,"c":9}', 'JSON(SKIP b, SKIP REGEXP \'^c\')')),
    (4, 42);

SELECT '-- shared variant';
SELECT id, dynamicType(d), dynamicElement(d, 'JSON(SKIP b)'), d.`JSON(SKIP b)`, d.`JSON(SKIP REGEXP \'^c\')`
FROM t_dyn_json_skip_shared ORDER BY id FORMAT TSV;

DROP TABLE t_dyn_json_skip_shared;
