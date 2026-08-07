DROP DICTIONARY IF EXISTS example_simple_key_dictionary;
CREATE DICTIONARY example_simple_key_dictionary (
    id UInt64,
    value UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE '' DATABASE currentDatabase()))
LAYOUT(DIRECT());

SELECT 'simple key';

SELECT name, key FROM system.dictionaries WHERE name='example_simple_key_dictionary' AND database=currentDatabase();

DROP DICTIONARY IF EXISTS example_complex_key_dictionary;
CREATE DICTIONARY example_complex_key_dictionary (
    id UInt64,
    id_key String,
    value UInt64
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE '' DATABASE currentDatabase()))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'complex key';

SELECT name, key FROM system.dictionaries WHERE name='example_complex_key_dictionary' AND database=currentDatabase();

DROP DICTIONARY example_complex_key_dictionary;
DROP DICTIONARY example_simple_key_dictionary;
