DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    `col1` DateTime,
    `col2` Int64,
    `col3` AggregateFunction(sumMap, Tuple(Array(UInt8), Array(UInt8)))
)
ENGINE = AggregatingMergeTree() ORDER BY (col1, col2);

SHOW CREATE TABLE test_table;
