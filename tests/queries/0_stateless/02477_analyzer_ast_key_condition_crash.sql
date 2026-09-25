SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (1);

SELECT * FROM test_table WHERE id = 1;

SELECT * FROM test_table WHERE id = 1;

DROP TABLE test_table;
