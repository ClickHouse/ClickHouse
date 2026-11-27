-- `allow_tuple_element_aggregation` must be disabled by default so that upgrading does not
-- change the behaviour of existing tables. With the default, plain Tuple columns are allowed
-- in the sorting key and Tuple columns are not aggregated element-wise during merges.

DROP TABLE IF EXISTS t_default_sorting_tuple;
DROP TABLE IF EXISTS t_default_no_aggregation;

SELECT 'Default value';
SELECT value FROM system.merge_tree_settings WHERE name = 'allow_tuple_element_aggregation';

SELECT 'Plain Tuple sorting key is allowed by default';
CREATE TABLE t_default_sorting_tuple
(
    n Tuple(a UInt32, b UInt32),
    v UInt64
)
ENGINE = SummingMergeTree
ORDER BY n;
DROP TABLE t_default_sorting_tuple;

SELECT 'Tuple columns are not aggregated element-wise by default';
CREATE TABLE t_default_no_aggregation
(
    id UInt32,
    metrics Tuple(a UInt64, b UInt64)
)
ENGINE = SummingMergeTree
ORDER BY id;

INSERT INTO t_default_no_aggregation VALUES (1, (10, 100)), (1, (20, 200));
SELECT id, metrics FROM t_default_no_aggregation FINAL ORDER BY id;

DROP TABLE t_default_no_aggregation;
