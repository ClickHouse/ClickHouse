SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table_1 VALUES (1, 'Value_1'), (2, 'Value_2');
INSERT INTO test_table_2 VALUES (2, 'Value_2'), (3, 'Value_3');

EXPLAIN header = 1, actions = 1 SELECT * FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 0;

SELECT '--';

SELECT * FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 0;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT * FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0;

SELECT '--';

SELECT * FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT * FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0;

SELECT '--';

SELECT * FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0;

DROP TABLE test_table_1;
DROP TABLE test_table_2;
