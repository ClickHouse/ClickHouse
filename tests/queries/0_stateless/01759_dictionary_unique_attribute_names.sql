-- Tags: no-parallel

DROP DATABASE IF EXISTS _01759_db;
CREATE DATABASE _01759_db;

CREATE TABLE _01759_db.dictionary_source_table
(
   key UInt64,
   value1 UInt64,
   value2 UInt64
)
ENGINE = TinyLog;

INSERT INTO _01759_db.dictionary_source_table VALUES (0, 2, 3), (1, 5, 6), (2, 8, 9);

CREATE DICTIONARY _01759_db.test_dictionary(key UInt64, value1 UInt64, value1 UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table' DB '_01759_db'))
LAYOUT(COMPLEX_KEY_DIRECT()); -- {serverError 36}

CREATE DICTIONARY _01759_db.test_dictionary(key UInt64, value1 UInt64, value2 UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table' DB '_01759_db'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT number, dictGet('_01759_db.test_dictionary', 'value1', tuple(number)) as value1,
   dictGet('_01759_db.test_dictionary', 'value2', tuple(number)) as value2 FROM system.numbers LIMIT 3;

DROP DATABASE _01759_db;
