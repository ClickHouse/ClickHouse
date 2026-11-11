-- A Tuple of arrays whose flattened name ends with `Map` is aggregated as a map during
-- merges. When an explicit `columns` list is given to SummingMergeTree, such a map must be
-- aggregated only if it (or its tuple ancestor) is listed; otherwise it must be left
-- untouched. Without an explicit list it is aggregated as before.

DROP TABLE IF EXISTS t_top_level_map_name;
DROP TABLE IF EXISTS t_nested_map_name;
DROP TABLE IF EXISTS t_map_name_no_columns;
DROP TABLE IF EXISTS t_dotted_element_names;

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

SELECT 'Dotted element names that merely look like a map path are not aggregated';
-- `ratesMap.ID` / `ratesMap.Value` are the element names; there is no real `ratesMap` tuple, so
-- the synthesized parent `metrics.ratesMap` is not a true ancestor and must not be map-merged.
CREATE TABLE t_dotted_element_names
(
    id UInt64,
    metrics Tuple(`ratesMap.ID` Array(UInt64), `ratesMap.Value` Array(UInt64))
)
ENGINE = SummingMergeTree
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_dotted_element_names VALUES (1, ([1], [10])), (1, ([1], [100]));
SELECT id, metrics FROM t_dotted_element_names FINAL ORDER BY id;

DROP TABLE t_top_level_map_name;
DROP TABLE t_nested_map_name;
DROP TABLE t_map_name_no_columns;
DROP TABLE t_dotted_element_names;
