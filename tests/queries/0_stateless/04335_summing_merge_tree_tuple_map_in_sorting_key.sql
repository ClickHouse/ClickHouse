-- Test: flattened xxxMap inside a Tuple whose key array is in the sorting key is excluded from summation
DROP TABLE IF EXISTS t_flat_map_in_sk;
DROP TABLE IF EXISTS t_flat_map_not_in_sk;

-- Map key sub-column in the sorting key: the map must NOT be summed (rows merge, first value kept).
CREATE TABLE t_flat_map_in_sk (
    id UInt64,
    metrics Tuple(ratesMap Tuple(ID Array(UInt64), Value Array(UInt64)))
) ENGINE = SummingMergeTree ORDER BY (id, metrics.ratesMap.ID)
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_flat_map_in_sk VALUES (1, (([1,2],[10,20]))), (1, (([1,2],[100,200])));
OPTIMIZE TABLE t_flat_map_in_sk FINAL;

SELECT 'map key in sorting key (excluded):';
SELECT id, metrics FROM t_flat_map_in_sk ORDER BY id;

-- Contrast: same data, map not in sorting key -> map is summed by key.
CREATE TABLE t_flat_map_not_in_sk (
    id UInt64,
    metrics Tuple(ratesMap Tuple(ID Array(UInt64), Value Array(UInt64)))
) ENGINE = SummingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_flat_map_not_in_sk VALUES (1, (([1,2],[10,20]))), (1, (([1,2],[100,200])));
OPTIMIZE TABLE t_flat_map_not_in_sk FINAL;

SELECT 'map not in sorting key (summed):';
SELECT id, metrics FROM t_flat_map_not_in_sk ORDER BY id;

DROP TABLE t_flat_map_in_sk;
DROP TABLE t_flat_map_not_in_sk;
