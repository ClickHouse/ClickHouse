-- With `restore_replace_external_dictionary_source_to_null`, a dictionary without `SOURCE` is rejected instead of crashing the server.

SET restore_replace_external_dictionary_source_to_null = 1;

-- The setting is in effect: an external source is replaced with `NULL()`.
CREATE DICTIONARY d_external (id UInt64) PRIMARY KEY id SOURCE(HTTP(URL 'http://127.0.0.1:1/' FORMAT 'TSV')) LAYOUT(FLAT()) LIFETIME(0);
SELECT create_table_query LIKE '%SOURCE(NULL())%' FROM system.tables WHERE database = currentDatabase() AND name = 'd_external';

CREATE DICTIONARY d_no_source (id UInt64) PRIMARY KEY id LAYOUT(FLAT()) LIFETIME(0); -- { serverError INCORRECT_DICTIONARY_DEFINITION }

DROP DICTIONARY d_external;
