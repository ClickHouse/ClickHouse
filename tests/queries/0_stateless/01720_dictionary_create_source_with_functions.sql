-- Tags: no-parallel

DROP DATABASE IF EXISTS _01720_dictionary_db;
CREATE DATABASE _01720_dictionary_db;

CREATE TABLE _01720_dictionary_db.dictionary_source_table
(
	key UInt8,
    value String
)
ENGINE = TinyLog;

INSERT INTO _01720_dictionary_db.dictionary_source_table VALUES (1, 'First');

CREATE DICTIONARY _01720_dictionary_db.dictionary
(
    key UInt64,
    value String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(DB '_01720_dictionary_db' TABLE 'dictionary_source_table' HOST hostName() PORT tcpPort()))
LIFETIME(0)
LAYOUT(FLAT());

SELECT * FROM _01720_dictionary_db.dictionary;

DROP DICTIONARY _01720_dictionary_db.dictionary;
DROP TABLE _01720_dictionary_db.dictionary_source_table;

DROP DATABASE _01720_dictionary_db;
