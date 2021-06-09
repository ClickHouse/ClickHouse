DROP TABLE IF EXISTS dictionary_array_source_table;
CREATE TABLE dictionary_array_source_table
(
    id UInt64,
    array_value Array(Int) DEFAULT [0, 1, 3]
) ENGINE=TinyLog;

INSERT INTO dictionary_array_source_table VALUES (0, [0, 1, 2]);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    id UInt64,
    array_value Array(Int)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_array_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(FLAT());

SELECT 'Flat dictionary';
SELECT dictGet('flat_dictionary', 'array_value', toUInt64(0));
DROP DICTIONARY flat_dictionary;

-- DROP DICTIONARY IF EXISTS hashed_dictionary;
-- CREATE DICTIONARY hashed_dictionary
-- (
--     id UInt64,
--     array_value Array(Int)
-- )
-- PRIMARY KEY id
-- SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_array_source_table'))
-- LIFETIME(MIN 1 MAX 1000)
-- LAYOUT(HASHED());

-- SELECT 'Hashed dictionary';
-- SELECT dictGet('hashed_dictionary', 'array_value', toUInt64(1));
-- DROP DICTIONARY hashed_dictionary;

-- DROP DICTIONARY IF EXISTS cache_dictionary;
-- CREATE DICTIONARY cache_dictionary
-- (
--     id UInt64,
--     array_value Array(Int)
-- )
-- PRIMARY KEY id
-- SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_array_source_table'))
-- LIFETIME(MIN 1 MAX 1000)
-- LAYOUT(CACHE(SIZE_IN_CELLS 10));

-- SELECT 'Cache dictionary';
-- SELECT dictGet('cache_dictionary', 'array_value', toUInt64(1));
-- DROP DICTIONARY cache_dictionary;

DROP TABLE dictionary_array_source_table;