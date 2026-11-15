SET enable_analyzer = 1;
SET single_join_prefer_left_table = 0;

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

DROP TABLE IF EXISTS test_table_join_3;
CREATE TABLE test_table_join_3
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_table_join_1 VALUES (0, 'Join_1_Value_0');
INSERT INTO test_table_join_1 VALUES (1, 'Join_1_Value_1');
INSERT INTO test_table_join_1 VALUES (3, 'Join_1_Value_3');

INSERT INTO test_table_join_2 VALUES (0, 'Join_2_Value_0');
INSERT INTO test_table_join_2 VALUES (1, 'Join_2_Value_1');
INSERT INTO test_table_join_2 VALUES (2, 'Join_2_Value_2');

INSERT INTO test_table_join_3 VALUES (0, 'Join_3_Value_0');
INSERT INTO test_table_join_3 VALUES (1, 'Join_3_Value_1');
INSERT INTO test_table_join_3 VALUES (2, 'Join_3_Value_2');

SELECT test_table_join_1.id, test_table_join_1.value, test_table_join_2.id, test_table_join_2.value
FROM test_table_join_1, test_table_join_2 ORDER BY ALL;

SELECT '--';

SELECT t1.id, t1.value, t2.id, t2.value FROM test_table_join_1 AS t1, test_table_join_2 AS t2 ORDER BY ALL;

SELECT '--';

SELECT t1.id, test_table_join_1.id, t1.value, test_table_join_1.value, t2.id, test_table_join_2.id, t2.value, test_table_join_2.value
FROM test_table_join_1 AS t1, test_table_join_2 AS t2 ORDER BY ALL;

SELECT '--';

SELECT t1.id, t1.value, t2.id, t2.value FROM test_table_join_1 AS t1, test_table_join_2 AS t2 ORDER BY ALL;

SELECT '--';

SELECT t1.id, test_table_join_1.id, t1.value, test_table_join_1.value, t2.id, test_table_join_2.id, t2.value, test_table_join_2.value FROM test_table_join_1 AS t1, test_table_join_2 AS t2 ORDER BY ALL;

SELECT '--';

SELECT test_table_join_1.id, test_table_join_1.value, test_table_join_2.id, test_table_join_2.value, test_table_join_3.id, test_table_join_3.value
FROM test_table_join_1, test_table_join_2, test_table_join_3 ORDER BY ALL;

SELECT '--';

SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM test_table_join_1 AS t1, test_table_join_2 AS t2, test_table_join_3 AS t3 ORDER BY ALL;

SELECT '--';

SELECT t1.id, test_table_join_1.id, t1.value, test_table_join_1.value, t2.id, test_table_join_2.id, t2.value, test_table_join_2.value,
t3.id, test_table_join_3.id, t3.value, test_table_join_3.value
FROM test_table_join_1 AS t1, test_table_join_2 AS t2, test_table_join_3 AS t3 ORDER BY ALL;

SELECT id FROM test_table_join_1, test_table_join_2; -- { serverError AMBIGUOUS_IDENTIFIER }

SELECT value FROM test_table_join_1, test_table_join_2; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE test_table_join_1;
DROP TABLE test_table_join_2;
DROP TABLE test_table_join_3;
