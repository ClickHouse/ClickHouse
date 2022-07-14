-- Tags: no-parallel

DROP DATABASE IF EXISTS _02015_db;
CREATE DATABASE _02015_db;

CREATE TABLE _02015_db.test_table
(
    key_column UInt64,
    data_column_1 UInt64,
    data_column_2 UInt8
)
ENGINE = MergeTree
ORDER BY key_column;

INSERT INTO _02015_db.test_table VALUES (0, 0, 0);

CREATE DICTIONARY _02015_db.test_dictionary
(
    key_column UInt64 DEFAULT 0,
    data_column_1 UInt64 DEFAULT 1,
    data_column_2 UInt8 DEFAULT 1
)
PRIMARY KEY key_column
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '_02015_db' TABLE 'test_table'));

CREATE TABLE _02015_db.test_table_default
(
    data_1 DEFAULT dictGetUInt64('_02015_db.test_dictionary', 'data_column_1', toUInt64(0)),
    data_2 DEFAULT dictGet(_02015_db.test_dictionary, 'data_column_2', toUInt64(0))
)
ENGINE=TinyLog;

INSERT INTO _02015_db.test_table_default(data_1) VALUES (5);
SELECT * FROM _02015_db.test_table_default;

DROP TABLE _02015_db.test_table_default;
DROP DICTIONARY _02015_db.test_dictionary;
DROP TABLE _02015_db.test_table;

DROP DATABASE _02015_db;
