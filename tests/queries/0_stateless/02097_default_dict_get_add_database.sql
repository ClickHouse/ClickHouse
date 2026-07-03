-- Tags: no-parallel, log-engine

DROP DATABASE IF EXISTS db_02097;
CREATE DATABASE db_02097;

USE db_02097;

CREATE TABLE test_table
(
    key_column UInt64,
    data_column_1 UInt64,
    data_column_2 UInt8
)
ENGINE = MergeTree
ORDER BY key_column;

CREATE DICTIONARY test_dictionary
(
    key_column UInt64 DEFAULT 0,
    data_column_1 UInt64 DEFAULT 1,
    data_column_2 UInt8 DEFAULT 1
)
PRIMARY KEY key_column
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'test_table'));

CREATE TABLE test_table_default
(
    data_1 DEFAULT dictGetUInt64('db_02097.test_dictionary', 'data_column_1', toUInt64(0)),
    data_2 DEFAULT dictGet(db_02097.test_dictionary, 'data_column_2', toUInt64(0))
)
ENGINE=TinyLog;

SELECT create_table_query FROM system.tables WHERE name = 'test_table_default' AND database = 'db_02097';

DROP TABLE test_table_default;
DROP DICTIONARY test_dictionary;
DROP TABLE test_table;

DROP DATABASE IF EXISTS db_02097;
