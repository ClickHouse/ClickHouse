-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS 01837_db;
CREATE DATABASE 01837_db ENGINE = Memory;

DROP TABLE IF EXISTS 01837_db.simple_key_dictionary_source;
CREATE TABLE 01837_db.simple_key_dictionary_source
(
    id UInt64,
    value String
) ENGINE = TinyLog;

INSERT INTO 01837_db.simple_key_dictionary_source VALUES (1, 'First');
INSERT INTO 01837_db.simple_key_dictionary_source VALUES (2, 'Second');
INSERT INTO 01837_db.simple_key_dictionary_source VALUES (3, 'Third');

DROP DICTIONARY IF EXISTS 01837_db.simple_key_direct_dictionary;
CREATE DICTIONARY 01837_db.simple_key_direct_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB '01837_db' TABLE 'simple_key_dictionary_source'))
LAYOUT(DIRECT());

SELECT * FROM 01837_db.simple_key_direct_dictionary;

DROP DICTIONARY 01837_db.simple_key_direct_dictionary;
DROP TABLE 01837_db.simple_key_dictionary_source;

DROP DATABASE 01837_db;
