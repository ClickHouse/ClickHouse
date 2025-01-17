DROP DICTIONARY IF EXISTS dictionary_with_map;

CREATE DICTIONARY dictionary_with_map
(
    `id` UInt64,
    `attribute` Map(String, String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY `SELECT 42 as key, map('key', 'value') AS attribute`))
LAYOUT(HASHED)
LIFETIME(10);

SELECT
    id,
    attribute['key'] AS hostname
FROM dictionary_with_map;
