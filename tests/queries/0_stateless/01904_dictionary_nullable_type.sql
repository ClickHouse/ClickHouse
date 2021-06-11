DROP TABLE IF EXISTS dictionary_nullable_source_table;
CREATE TABLE dictionary_nullable_source_table
(
    id UInt64,
    value Nullable(Int64)
) ENGINE=TinyLog;

INSERT INTO dictionary_nullable_source_table VALUES (0, 0), (1, NULL);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    id UInt64,
    value Nullable(Int64) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_nullable_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(FLAT());

SELECT 'Flat dictionary';
-- SELECT dictGet('flat_dictionary', 'value', toUInt64(0));
-- SELECT dictGet('flat_dictionary', 'value', toUInt64(1));
-- SELECT dictGet('flat_dictionary', 'value', toUInt64(2));
SELECT dictGetOrDefault('flat_dictionary', 'value', toUInt64(2), NULL);
DROP DICTIONARY flat_dictionary;
