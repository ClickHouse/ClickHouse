DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    `col1` DateTime,
    `col2` Int64,
    `col3` AggregateFunction(sumMap, Tuple(Array(UInt8), Array(UInt8)))
)
ENGINE = AggregatingMergeTree() ORDER BY (col1, col2);

SHOW CREATE TABLE test_table;

-- regression from performance tests comparison script
DROP TABLE IF EXISTS test;
CREATE TABLE test
ENGINE = Null AS
WITH (
        SELECT arrayReduce('sumMapState', [(['foo'], arrayMap(x -> -0., ['foo']))])
    ) AS all_metrics
SELECT
    (finalizeAggregation(arrayReduce('sumMapMergeState', [all_metrics])) AS metrics_tuple).1 AS metric_names,
    metrics_tuple.2 AS metric_values
FROM system.one;
