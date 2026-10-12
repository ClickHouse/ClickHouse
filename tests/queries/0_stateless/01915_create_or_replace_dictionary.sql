-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Atomic;
USE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_1
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_1 VALUES (0, 'Value0');

DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary;
CREATE OR REPLACE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'test_source_table_1'));

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_2
(
    id UInt64,
    value_1 String
) ENGINE=TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_2 VALUES (0, 'Value1');

CREATE OR REPLACE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary
(
    id UInt64,
    value_1 String
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'test_source_table_2'))
LIFETIME(0);

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_1;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_source_table_2;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
