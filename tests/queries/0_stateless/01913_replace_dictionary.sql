-- Tags: no-parallel

DROP DATABASE IF EXISTS _01913_db;
CREATE DATABASE _01913_db ENGINE=Atomic;

DROP TABLE IF EXISTS _01913_db.test_source_table_1;
CREATE TABLE _01913_db.test_source_table_1
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO _01913_db.test_source_table_1 VALUES (0, 'Value0');

DROP DICTIONARY IF EXISTS _01913_db.test_dictionary;
CREATE DICTIONARY _01913_db.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '_01913_db' TABLE 'test_source_table_1'));

SELECT * FROM _01913_db.test_dictionary;

DROP TABLE IF EXISTS _01913_db.test_source_table_2;
CREATE TABLE _01913_db.test_source_table_2
(
    id UInt64,
    value_1 String
) ENGINE=TinyLog;

INSERT INTO _01913_db.test_source_table_2 VALUES (0, 'Value1');

REPLACE DICTIONARY _01913_db.test_dictionary
(
    id UInt64,
    value_1 String
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(DB '_01913_db' TABLE 'test_source_table_2'))
LIFETIME(0);

SELECT * FROM _01913_db.test_dictionary;

DROP DICTIONARY _01913_db.test_dictionary;

DROP TABLE _01913_db.test_source_table_1;
DROP TABLE _01913_db.test_source_table_2;

DROP DATABASE _01913_db;
