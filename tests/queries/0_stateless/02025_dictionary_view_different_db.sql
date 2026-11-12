-- Tags: no-parallel

DROP DATABASE IF EXISTS test_db_2025;
CREATE DATABASE test_db_2025;

DROP TABLE IF EXISTS test_db_2025.test_table;
CREATE TABLE test_db_2025.test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_db_2025.test_table VALUES (0, 'Value');

CREATE DICTIONARY test_db_2025.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'test_table' DB 'test_db_2025'));

DROP TABLE IF EXISTS test_db_2025.view_table;
CREATE TABLE test_db_2025.view_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_db_2025.view_table VALUES (0, 'ViewValue');

DROP VIEW IF EXISTS test_view_different_db;
CREATE VIEW test_view_different_db AS SELECT id, value, dictGet('test_db_2025.test_dictionary', 'value', id) FROM test_db_2025.view_table;
SELECT * FROM test_view_different_db;

DROP DICTIONARY test_db_2025.test_dictionary;
DROP TABLE test_db_2025.test_table;
DROP TABLE test_db_2025.view_table;

DROP VIEW test_view_different_db;

DROP DATABASE test_db_2025;
