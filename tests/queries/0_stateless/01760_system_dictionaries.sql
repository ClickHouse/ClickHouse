-- Tags: no-parallel

DROP DATABASE IF EXISTS 01760_db;
CREATE DATABASE 01760_db;

DROP TABLE IF EXISTS 01760_db.example_simple_key_source;
CREATE TABLE 01760_db.example_simple_key_source (id UInt64, value UInt64) ENGINE=TinyLog;
INSERT INTO 01760_db.example_simple_key_source VALUES (0, 0), (1, 1), (2, 2);

DROP DICTIONARY IF EXISTS 01760_db.example_simple_key_dictionary;
CREATE DICTIONARY 01760_db.example_simple_key_dictionary (
    id UInt64,
    value UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'example_simple_key_source' DATABASE '01760_db'))
LAYOUT(DIRECT());

SELECT 'simple key';

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';
SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';

SELECT * FROM 01760_db.example_simple_key_dictionary;

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';

DROP DICTIONARY 01760_db.example_simple_key_dictionary;
DROP TABLE 01760_db.example_simple_key_source;

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';

DROP TABLE IF EXISTS 01760_db.example_complex_key_source;
CREATE TABLE 01760_db.example_complex_key_source (id UInt64, id_key String, value UInt64) ENGINE=TinyLog;
INSERT INTO 01760_db.example_complex_key_source VALUES (0, '0_key', 0), (1, '1_key', 1), (2, '2_key', 2);

DROP DICTIONARY IF EXISTS 01760_db.example_complex_key_dictionary;
CREATE DICTIONARY 01760_db.example_complex_key_dictionary (
    id UInt64,
    id_key String,
    value UInt64
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'example_complex_key_source' DATABASE '01760_db'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'complex key';

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';
SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';

SELECT * FROM 01760_db.example_complex_key_dictionary;

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='01760_db';

DROP DICTIONARY 01760_db.example_complex_key_dictionary;
DROP TABLE 01760_db.example_complex_key_source;

DROP DATABASE 01760_db;
