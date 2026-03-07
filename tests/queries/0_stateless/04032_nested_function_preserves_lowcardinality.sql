-- Tags: no-fasttest
-- Verify that the nested() function preserves LowCardinality in column types.
-- See https://github.com/ClickHouse/ClickHouse/issues/95582

SELECT 'nested with LowCardinality(String)';
SELECT toTypeName(nested(['name'], cast(['a', 'b'] as Array(LowCardinality(String))) as a));

SELECT 'nested with Array(LowCardinality(String))';
SELECT toTypeName(nested(['name'], cast([['a', 'b'], ['c']] as Array(Array(LowCardinality(String)))) as a));

SELECT 'ARRAY JOIN preserves LowCardinality';
DROP TABLE IF EXISTS test_lc_array_join;
CREATE TABLE test_lc_array_join
(
    `timestamp` DateTime,
    `stackMap.stack` Array(Array(LowCardinality(String))),
    `stackMap.value` Array(UInt64)
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
PRIMARY KEY timestamp;

INSERT INTO test_lc_array_join VALUES ('2024-01-01', [['frame1', 'frame2'], ['frame3']], [1, 2]);

SELECT toTypeName(stackMap.stack) FROM test_lc_array_join ARRAY JOIN stackMap;

DROP TABLE test_lc_array_join;
