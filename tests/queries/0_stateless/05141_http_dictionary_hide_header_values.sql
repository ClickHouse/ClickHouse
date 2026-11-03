-- The values of custom HTTP headers of an `HTTP` dictionary source often carry credentials,
-- so they must be hidden in `SHOW CREATE DICTIONARY`, `system.tables` and `system.query_log`,
-- the same way as the password. They are hidden as a whole, header names included.

SET format_display_secrets_in_show_and_select = 0;

DROP DICTIONARY IF EXISTS d_05141;
CREATE DICTIONARY d_05141 (id UInt64, v String)
PRIMARY KEY id
SOURCE(HTTP(
    url 'http://localhost:11111/x.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'SEKRIT_PW')
    headers(
        header(name 'API-KEY' value 'SEKRIT_TOKEN_1')
        header(name 'X-Other' value 'SEKRIT_TOKEN_2')
    )
))
LIFETIME(0) LAYOUT(FLAT());

SHOW CREATE DICTIONARY d_05141;

SELECT create_table_query LIKE concat('%', 'SEKRIT', '%'), create_table_query LIKE '%API-KEY%', create_table_query LIKE '%HEADERS (\'[HIDDEN]\')%'
FROM system.tables WHERE database = currentDatabase() AND name = 'd_05141';

DROP DICTIONARY d_05141;

-- The query is logged before the dictionary source validates its structure, so malformed header
-- definitions must not leak either.
CREATE DICTIONARY d_05141_typo (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(header(name 'API-KEY' vaule 'SEKRIT_TYPO'))))
LIFETIME(0) LAYOUT(FLAT());
CREATE DICTIONARY d_05141_key (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(header(secret 'SEKRIT_KEY'))))
LIFETIME(0) LAYOUT(FLAT());
CREATE DICTIONARY d_05141_nested (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(header(name(foo 'SEKRIT_NESTED')))))
LIFETIME(0) LAYOUT(FLAT());
CREATE DICTIONARY d_05141_func (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(header(name concat('X-', 'SEKRIT_FUNC') value 'SEKRIT_FUNC_VALUE'))))
LIFETIME(0) LAYOUT(FLAT()); -- { serverError INCORRECT_DICTIONARY_DEFINITION }
CREATE DICTIONARY d_05141_nobr (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(header 'SEKRIT_NOBR')))
LIFETIME(0) LAYOUT(FLAT());
CREATE DICTIONARY d_05141_foo (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers(foo 'SEKRIT_FOO')))
LIFETIME(0) LAYOUT(FLAT());
CREATE DICTIONARY d_05141_flat (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost:11111/x.tsv' format 'TabSeparated' headers 'SEKRIT_HEADERS'))
LIFETIME(0) LAYOUT(FLAT());

SELECT name, extract(create_table_query, 'HEADERS.*$')
FROM system.tables WHERE database = currentDatabase() AND name LIKE 'd\\_05141\\_%' ORDER BY name;

SYSTEM FLUSH LOGS query_log;
SELECT countIf(query LIKE '%d\\_05141 %'), countIf(query LIKE '%d\\_05141\\_%'), countIf(query LIKE concat('%', 'SEKRIT', '%'))
FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create' AND event_date >= yesterday();

DROP DICTIONARY d_05141_typo;
DROP DICTIONARY d_05141_key;
DROP DICTIONARY d_05141_nested;
DROP DICTIONARY d_05141_nobr;
DROP DICTIONARY d_05141_foo;
DROP DICTIONARY d_05141_flat;
