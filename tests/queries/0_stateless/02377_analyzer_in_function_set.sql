SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value_0'), (1, 'Value_1'), (2, 'Value_2');

DROP TABLE IF EXISTS special_set_table;
CREATE TABLE special_set_table
(
    id UInt64
) ENGINE=Set;

INSERT INTO special_set_table VALUES (0), (1);

SELECT id, value FROM test_table WHERE id IN special_set_table;

DROP TABLE special_set_table;
DROP TABLE test_table;
