-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS _01837_db;
CREATE DATABASE _01837_db ENGINE = Memory;

DROP TABLE IF EXISTS _01837_db.simple_key_dictionary_source;
CREATE TABLE _01837_db.simple_key_dictionary_source
(
    id UInt64,
    value String
) ENGINE = TinyLog;

INSERT INTO _01837_db.simple_key_dictionary_source VALUES (1, 'First');
INSERT INTO _01837_db.simple_key_dictionary_source VALUES (2, 'Second');
INSERT INTO _01837_db.simple_key_dictionary_source VALUES (3, 'Third');

DROP DICTIONARY IF EXISTS _01837_db.simple_key_direct_dictionary;
CREATE DICTIONARY _01837_db.simple_key_direct_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB '_01837_db' TABLE 'simple_key_dictionary_source'))
LAYOUT(DIRECT());

SELECT * FROM _01837_db.simple_key_direct_dictionary;

DROP DICTIONARY _01837_db.simple_key_direct_dictionary;
DROP TABLE _01837_db.simple_key_dictionary_source;

DROP DATABASE _01837_db;
