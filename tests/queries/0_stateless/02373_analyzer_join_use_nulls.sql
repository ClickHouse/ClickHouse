SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS test_table_join_1;
CREATE TABLE test_table_join_1
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY tuple();

DROP TABLE IF EXISTS test_table_join_2;
CREATE TABLE test_table_join_2
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_table_join_1 VALUES (0, 'Join_1_Value_0');
INSERT INTO test_table_join_1 VALUES (1, 'Join_1_Value_1');
INSERT INTO test_table_join_1 VALUES (2, 'Join_1_Value_2');

INSERT INTO test_table_join_2 VALUES (0, 'Join_2_Value_0');
INSERT INTO test_table_join_2 VALUES (1, 'Join_2_Value_1');
INSERT INTO test_table_join_2 VALUES (3, 'Join_2_Value_3');

-- { echoOn }

SELECT t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value), t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY ALL;

SELECT '--';

SELECT t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value), t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 LEFT JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY ALL;

SELECT '--';

SELECT t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value), t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 RIGHT JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY ALL;

SELECT '--';

SELECT t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value), t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 FULL JOIN test_table_join_2 AS t2 ON t1.id = t2.id ORDER BY ALL;

SELECT '--';

SELECT id AS using_id, toTypeName(using_id), t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value),
t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) ORDER BY ALL;

SELECT '--';

SELECT id AS using_id, toTypeName(using_id), t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value),
t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 LEFT JOIN test_table_join_2 AS t2 USING (id) ORDER BY ALL;

SELECT '--';

SELECT id AS using_id, toTypeName(using_id), t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value),
t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 RIGHT JOIN test_table_join_2 AS t2 USING (id) ORDER BY ALL;

SELECT '--';

SELECT id AS using_id, toTypeName(using_id), t1.id AS t1_id, toTypeName(t1_id), t1.value AS t1_value, toTypeName(t1_value),
t2.id AS t2_id, toTypeName(t2_id), t2.value AS t2_value, toTypeName(t2_value)
FROM test_table_join_1 AS t1 FULL JOIN test_table_join_2 AS t2 USING (id) ORDER BY ALL;

-- { echoOff }

DROP TABLE test_table_join_1;
DROP TABLE test_table_join_2;
