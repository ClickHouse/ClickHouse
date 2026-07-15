-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.test_table;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.test_table VALUES (0, 'Value');

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'test_table' DB currentDatabase()));

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.view_table;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.view_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.view_table VALUES (0, 'ViewValue');

DROP VIEW IF EXISTS test_view_different_db;
CREATE VIEW test_view_different_db AS SELECT id, value, dictGet('test_dictionary', 'value', id) FROM view_table;
SELECT * FROM test_view_different_db;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_table;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.view_table;

DROP VIEW test_view_different_db;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
