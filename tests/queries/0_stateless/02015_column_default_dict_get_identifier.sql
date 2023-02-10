-- Tags: no-parallel

DROP DATABASE IF EXISTS 02015_db;
CREATE DATABASE 02015_db;

CREATE TABLE 02015_db.test_table
(
    key_column UInt64,
    data_column_1 UInt64,
    data_column_2 UInt8
)
ENGINE = MergeTree
ORDER BY key_column;

INSERT INTO 02015_db.test_table VALUES (0, 0, 0);

CREATE DICTIONARY 02015_db.test_dictionary
(
    key_column UInt64 DEFAULT 0,
    data_column_1 UInt64 DEFAULT 1,
    data_column_2 UInt8 DEFAULT 1
)
PRIMARY KEY key_column
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '02015_db' TABLE 'test_table'));

CREATE TABLE 02015_db.test_table_default
(
    data_1 DEFAULT dictGetUInt64('02015_db.test_dictionary', 'data_column_1', toUInt64(0)),
    data_2 DEFAULT dictGet(02015_db.test_dictionary, 'data_column_2', toUInt64(0))
)
ENGINE=TinyLog;

INSERT INTO 02015_db.test_table_default(data_1) VALUES (5);
SELECT * FROM 02015_db.test_table_default;

DROP TABLE 02015_db.test_table_default;
DROP DICTIONARY 02015_db.test_dictionary;
DROP TABLE 02015_db.test_table;

DROP DATABASE 02015_db;
