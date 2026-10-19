-- Tags: no-old-analyzer

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (test_column Int32) ENGINE = MergeTree ORDER BY test_column;

INSERT INTO test_table SELECT 1;
INSERT INTO test_table SELECT 2;

SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.test_table WHERE {CLICKHOUSE_DATABASE:Identifier}.test_table.test_column = 1;

DELETE FROM {CLICKHOUSE_DATABASE:Identifier}.test_table WHERE {CLICKHOUSE_DATABASE:Identifier}.test_table.test_column = 1;

SELECT * FROM test_table ORDER BY test_column;

DROP TABLE test_table;
