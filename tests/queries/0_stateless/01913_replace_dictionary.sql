DROP DATABASE IF EXISTS 01913_db;
CREATE DATABASE 01913_db ENGINE=Atomic;

DROP TABLE IF EXISTS 01913_db.test_source_table_1;
CREATE TABLE 01913_db.test_source_table_1
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO 01913_db.test_source_table_1 VALUES (0, 'Value0');

DROP DICTIONARY IF EXISTS 01913_db.test_dictionary;
CREATE DICTIONARY 01913_db.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '01913_db' TABLE 'test_source_table_1'));

SELECT * FROM 01913_db.test_dictionary;

DROP TABLE IF EXISTS 01913_db.test_source_table_2;
CREATE TABLE 01913_db.test_source_table_2
(
    id UInt64,
    value_1 String
) ENGINE=TinyLog;

INSERT INTO 01913_db.test_source_table_2 VALUES (0, 'Value1');

REPLACE DICTIONARY 01913_db.test_dictionary
(
    id UInt64,
    value_1 String
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(DB '01913_db' TABLE 'test_source_table_2'))
LIFETIME(0);

SELECT * FROM 01913_db.test_dictionary;

DROP DICTIONARY 01913_db.test_dictionary;

DROP TABLE 01913_db.test_source_table_1;
DROP TABLE 01913_db.test_source_table_2;

DROP DATABASE 01913_db;
