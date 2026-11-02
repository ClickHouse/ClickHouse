-- Test ARRAY JOIN over a Nested prefix where a same-prefixed column is NOT an Array:
-- the scalar sibling must be skipped and only the Array per-field columns expanded.

DROP TABLE IF EXISTS t_nested_prefix_non_array SYNC;

CREATE TABLE t_nested_prefix_non_array
(
    id String,
    `loc.x` Array(String),
    `loc.y` Array(String),
    `loc.z` String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_nested_prefix_non_array (id, `loc.x`, `loc.y`, `loc.z`)
VALUES ('a', ['x1', 'x2'], ['y1', 'y2'], 'scalar');

SELECT loc.x, loc.y FROM t_nested_prefix_non_array ARRAY JOIN loc ORDER BY loc.x
SETTINGS enable_analyzer = 1;

SELECT loc.x, loc.z FROM t_nested_prefix_non_array ARRAY JOIN loc ORDER BY loc.x
SETTINGS enable_analyzer = 1;

DROP TABLE t_nested_prefix_non_array SYNC;
