DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (n Int32, s String)
ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

INSERT INTO test_table SELECT number, toString(number) FROM system.numbers LIMIT 100;
INSERT INTO test_table SELECT number, toString(number * number) FROM system.numbers LIMIT 100;
INSERT INTO test_table SELECT number, toString(number * number) FROM system.numbers LIMIT 100;

SELECT * FROM test_table ORDER BY n, s LIMIT 30;

DROP TABLE test_table;
