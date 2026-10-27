SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table_join_1;
CREATE TABLE test_table_join_1
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

DROP TABLE IF EXISTS test_table_join_2;
CREATE TABLE test_table_join_2
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table_join_1 VALUES (0, 'Join_1_Value_0');
INSERT INTO test_table_join_1 VALUES (1, 'Join_1_Value_1');
INSERT INTO test_table_join_1 VALUES (2, 'Join_1_Value_2');

INSERT INTO test_table_join_2 VALUES (0, 'Join_2_Value_0');
INSERT INTO test_table_join_2 VALUES (1, 'Join_2_Value_1');
INSERT INTO test_table_join_2 VALUES (3, 'Join_2_Value_3');

-- { echoOn }

SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY t1.id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 LEFT JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY t1.id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 RIGHT JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY t1.id, t1.value;

SELECT '--';

SELECT * FROM test_table_join_1 AS t1 FULL JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY t1.id, t1.value;

-- { echoOff }

DROP TABLE test_table_join_1;
DROP TABLE test_table_join_2;
