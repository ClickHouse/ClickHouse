-- The values of custom HTTP headers of an `HTTP` dictionary source often carry credentials,
-- so they must be hidden in `SHOW CREATE DICTIONARY`, `system.tables` and `system.query_log`,
-- the same way as the password. The header names stay visible.

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

SELECT create_table_query LIKE '%SEKRIT%', create_table_query LIKE '%NAME \'API-KEY\' VALUE \'[HIDDEN]\'%', create_table_query LIKE '%NAME \'X-Other\' VALUE \'[HIDDEN]\'%'
FROM system.tables WHERE database = currentDatabase() AND name = 'd_05141';

DROP DICTIONARY d_05141;

SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, countIf(query LIKE '%SEKRIT%')
FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create' AND query LIKE '%d_05141%' AND event_date >= yesterday();
