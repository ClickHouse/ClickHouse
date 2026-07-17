SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1
(
    id UInt64,
    value_1 String,
    value_2 UInt64
) ENGINE=MergeTree ORDER BY id;

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2
(
    id UInt64,
    value_1 String,
    value_2 UInt64
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table_1 VALUES (0, 'Value', 0);
INSERT INTO test_table_2 VALUES (0, 'Value', 0);

SET query_plan_join_swap_table = 'false';

EXPLAIN header = 1, actions = 1 SELECT lhs.id, lhs.value_1, rhs.id, rhs.value_1
FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT lhs.id, lhs.value_1, rhs.id, rhs.value_1
FROM test_table_1 AS lhs ASOF JOIN test_table_2 AS rhs ON lhs.id = rhs.id AND lhs.value_2 < rhs.value_2;

DROP TABLE test_table_1;
DROP TABLE test_table_2;
