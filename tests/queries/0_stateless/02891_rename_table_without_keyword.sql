DROP DATABASE IF EXISTS rename_db;
CREATE DATABASE IF NOT EXISTS rename_db;

CREATE TABLE IF NOT EXISTS rename_db.r1 (name String) Engine=Memory();
SHOW TABLES FROM rename_db;

RENAME TABLE rename_db.r1 TO rename_db.r2;
SHOW TABLES FROM rename_db;

RENAME rename_db.r2 TO rename_db.r3;
SHOW TABLES FROM rename_db;

CREATE TABLE IF NOT EXISTS rename_db.source_table (
    id UInt64,
    value String
) ENGINE = Memory;

CREATE DICTIONARY IF NOT EXISTS rename_db.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'rename_db.dictionary_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SHOW DICTIONARIES FROM rename_db;

RENAME rename_db.test_dictionary TO rename_db.test_dictionary_2; -- { serverError UNKNOWN_TABLE }
SHOW DICTIONARIES FROM rename_db;

SHOW DATABASES LIKE 'rename_db';
RENAME rename_db TO rename_db_2; -- { serverError UNKNOWN_TABLE }
SHOW DATABASES LIKE 'rename_db';

DROP DATABASE IF EXISTS rename_db;

