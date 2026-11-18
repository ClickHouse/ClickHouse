SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value_0');

SET max_columns_to_read = 1;

SELECT id FROM (SELECT * FROM test_table);
SELECT id FROM (SELECT * FROM (SELECT * FROM test_table));
SELECT id FROM (SELECT * FROM test_table UNION ALL SELECT * FROM test_table);

SELECT id FROM (SELECT id, value FROM test_table);
SELECT id FROM (SELECT id, value FROM (SELECT id, value FROM test_table));
SELECT id FROM (SELECT id, value FROM test_table UNION ALL SELECT id, value FROM test_table);

DROP TABLE test_table;
