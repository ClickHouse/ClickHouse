-- Tags: no-parallel

DROP DATABASE IF EXISTS _01760_db;
CREATE DATABASE _01760_db;

DROP TABLE IF EXISTS _01760_db.example_simple_key_source;
CREATE TABLE _01760_db.example_simple_key_source (id UInt64, value UInt64) ENGINE=TinyLog;
INSERT INTO _01760_db.example_simple_key_source VALUES (0, 0), (1, 1), (2, 2);

DROP DICTIONARY IF EXISTS _01760_db.example_simple_key_dictionary;
CREATE DICTIONARY _01760_db.example_simple_key_dictionary (
    id UInt64,
    value UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'example_simple_key_source' DATABASE '_01760_db'))
LAYOUT(DIRECT());

SELECT 'simple key';

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';
SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';

SELECT * FROM _01760_db.example_simple_key_dictionary;

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';

DROP TABLE _01760_db.example_simple_key_source;
DROP DICTIONARY _01760_db.example_simple_key_dictionary;

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';

DROP TABLE IF EXISTS _01760_db.example_complex_key_source;
CREATE TABLE _01760_db.example_complex_key_source (id UInt64, id_key String, value UInt64) ENGINE=TinyLog;
INSERT INTO _01760_db.example_complex_key_source VALUES (0, '0_key', 0), (1, '1_key', 1), (2, '2_key', 2);

DROP DICTIONARY IF EXISTS _01760_db.example_complex_key_dictionary;
CREATE DICTIONARY _01760_db.example_complex_key_dictionary (
    id UInt64,
    id_key String,
    value UInt64
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'example_complex_key_source' DATABASE '_01760_db'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'complex key';

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';
SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';

SELECT * FROM _01760_db.example_complex_key_dictionary;

SELECT name, database, key.names, key.types, attribute.names, attribute.types, status FROM system.dictionaries WHERE database='_01760_db';

DROP TABLE _01760_db.example_complex_key_source;
DROP DICTIONARY _01760_db.example_complex_key_dictionary;

DROP DATABASE _01760_db;
