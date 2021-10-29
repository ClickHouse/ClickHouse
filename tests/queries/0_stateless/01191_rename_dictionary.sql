-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01191;
CREATE DATABASE test_01191 ENGINE=Atomic;

CREATE TABLE test_01191._ (n UInt64, s String) ENGINE = Memory();

CREATE DICTIONARY test_01191.dict (n UInt64, s String)
PRIMARY KEY n
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE '_' DB 'test_01191'));

INSERT INTO test_01191._ VALUES (42, 'test');

SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;

RENAME DICTIONARY test_01191.table TO test_01191.table1; -- {serverError 60}
EXCHANGE TABLES test_01191.table AND test_01191.dict; -- {serverError 60}
EXCHANGE TABLES test_01191.dict AND test_01191.table; -- {serverError 80}
RENAME TABLE test_01191.dict TO test_01191.dict1; -- {serverError 80}

CREATE DATABASE dummy_db ENGINE=Atomic;
RENAME DICTIONARY test_01191.dict TO dummy_db.dict1;
RENAME DICTIONARY dummy_db.dict1 TO test_01191.dict;
DROP DATABASE dummy_db;

RENAME DICTIONARY test_01191.dict TO test_01191.dict1;

SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;
SELECT dictGet(test_01191.dict1, 's', toUInt64(42));

RENAME DICTIONARY test_01191.dict1 TO test_01191.dict2;

SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;
SELECT dictGet(test_01191.dict2, 's', toUInt64(42));

DROP DATABASE test_01191;
