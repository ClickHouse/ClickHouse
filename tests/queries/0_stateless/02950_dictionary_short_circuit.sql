-- Tags: no-parallel

DROP TABLE IF EXISTS dictionary_source_table;
CREATE TABLE dictionary_source_table
(
    id UInt64,
    v1 String,
    v2 Nullable(String)
) ENGINE=TinyLog;

INSERT INTO dictionary_source_table VALUES (0, 'zero', 'zero'), (1, 'one', NULL);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT 'Flat dictionary';
SELECT dictGetOrDefault('flat_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
DROP DICTIONARY flat_dictionary;


DROP DICTIONARY IF EXISTS hashed_dictionary;
CREATE DICTIONARY hashed_dictionary
(
    id UInt64,
    v1 String,
    v2 Nullable(String) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dictionary_source_table'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

SELECT 'Hashed dictionary';
SELECT dictGetOrDefault('hashed_dictionary', ('v1', 'v2'), 0, (intDiv(1, id), intDiv(1, id)))
FROM dictionary_source_table;
DROP DICTIONARY hashed_dictionary;