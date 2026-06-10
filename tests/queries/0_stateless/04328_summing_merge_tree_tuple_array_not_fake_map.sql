-- A Tuple of arrays whose flattened name ends with `Map` is aggregated as a map during
-- merges. When an explicit `columns` list is given to SummingMergeTree, such a map must be
-- aggregated only if it (or its tuple ancestor) is listed; otherwise it must be left
-- untouched. Without an explicit list it is aggregated as before.

DROP TABLE IF EXISTS t_top_level_map_name;
DROP TABLE IF EXISTS t_nested_map_name;
DROP TABLE IF EXISTS t_map_name_no_columns;

SELECT 'Map-named tuple of arrays with explicit columns is not aggregated';
CREATE TABLE t_top_level_map_name
(
    id UInt64,
    ratesMap Tuple(ID Array(UInt64), Value Array(UInt64)),
    other UInt64
)
ENGINE = SummingMergeTree(other)
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_top_level_map_name VALUES (1, ([1], [10]), 5), (1, ([1], [100]), 7);
SELECT id, ratesMap, other FROM t_top_level_map_name FINAL ORDER BY id;

SELECT 'Nested map-named tuple of arrays with explicit columns is not aggregated';
CREATE TABLE t_nested_map_name
(
    id UInt64,
    metrics Tuple(ratesMap Tuple(ID Array(UInt64), Value Array(UInt64))),
    other UInt64
)
ENGINE = SummingMergeTree(other)
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_nested_map_name VALUES (1, (([1], [10])), 5), (1, (([1], [100])), 7);
SELECT id, metrics, other FROM t_nested_map_name FINAL ORDER BY id;

SELECT 'Map-named tuple of arrays without explicit columns is aggregated';
CREATE TABLE t_map_name_no_columns
(
    id UInt64,
    ratesMap Tuple(ID Array(UInt64), Value Array(UInt64))
)
ENGINE = SummingMergeTree
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_map_name_no_columns VALUES (1, ([1], [10])), (1, ([1], [100]));
SELECT id, ratesMap FROM t_map_name_no_columns FINAL ORDER BY id;

DROP TABLE t_top_level_map_name;
DROP TABLE t_nested_map_name;
DROP TABLE t_map_name_no_columns;
