DROP TABLE IF EXISTS test_tuple_element;
CREATE TABLE test_tuple_element
(
    tuple Tuple(k1 Nullable(UInt64), k2 UInt64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO test_tuple_element VALUES (tuple(1,2)), (tuple(NULL, 3));

SELECT
    tupleElement(tuple, 'k1', 0) fine_k1_with_0,
    tupleElement(tuple, 'k1', NULL) k1_with_null,
    tupleElement(tuple, 'k2', 0) k2_with_0,
    tupleElement(tuple, 'k2', NULL) k2_with_null
FROM test_tuple_element;

DROP TABLE test_tuple_element;
