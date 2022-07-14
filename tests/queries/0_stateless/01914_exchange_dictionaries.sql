-- Tags: no-ordinary-database, no-parallel
-- Tag no-ordinary-database: Requires Atomic database

DROP DATABASE IF EXISTS _01914_db;
CREATE DATABASE _01914_db ENGINE=Atomic;

DROP TABLE IF EXISTS _01914_db.table_1;
CREATE TABLE _01914_db.table_1 (id UInt64, value String) ENGINE=TinyLog;

DROP TABLE IF EXISTS _01914_db.table_2;
CREATE TABLE _01914_db.table_2 (id UInt64, value String) ENGINE=TinyLog;

INSERT INTO _01914_db.table_1 VALUES (1, 'Table1');
INSERT INTO _01914_db.table_2 VALUES (2, 'Table2');

DROP DICTIONARY IF EXISTS _01914_db.dictionary_1;
CREATE DICTIONARY _01914_db.dictionary_1 (id UInt64, value String)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '_01914_db' TABLE 'table_1'));

DROP DICTIONARY IF EXISTS _01914_db.dictionary_2;
CREATE DICTIONARY _01914_db.dictionary_2 (id UInt64, value String)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '_01914_db' TABLE 'table_2'));

SELECT * FROM _01914_db.dictionary_1;
SELECT * FROM _01914_db.dictionary_2;

EXCHANGE DICTIONARIES _01914_db.dictionary_1 AND _01914_db.dictionary_2;

SELECT * FROM _01914_db.dictionary_1;
SELECT * FROM _01914_db.dictionary_2;

DROP DICTIONARY _01914_db.dictionary_1;
DROP DICTIONARY _01914_db.dictionary_2;

DROP TABLE _01914_db.table_1;
DROP TABLE _01914_db.table_2;

DROP DATABASE _01914_db;
