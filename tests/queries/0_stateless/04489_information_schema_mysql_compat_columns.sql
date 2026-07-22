-- Verify the MySQL-compatibility columns exposed by INFORMATION_SCHEMA views,
-- so that MySQL-aware clients (JDBC/ODBC drivers, IDEs, BI tools) can issue their
-- MySQL-flavored catalog queries without hitting UNKNOWN_IDENTIFIER.

DROP TABLE IF EXISTS test_infoschema_compat;

SELECT '-- schemata: the exact MySQL-dialect introspection query';
SELECT DEFAULT_COLLATION_NAME, DEFAULT_ENCRYPTION
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = 'information_schema';

SELECT '-- schemata: lowercase aliases resolve too';
SELECT default_collation_name, default_encryption
FROM information_schema.schemata
WHERE schema_name = 'INFORMATION_SCHEMA';

CREATE TABLE test_infoschema_compat
(
    id UInt32,
    s String,
    m UInt32 MATERIALIZED id * 2,
    a UInt32 ALIAS id + 1
)
ENGINE = MergeTree ORDER BY id;

SELECT '-- tables: MySQL-specific columns';
SELECT engine, version, row_format, avg_row_length, max_data_length,
       data_free, auto_increment, create_time, update_time, check_time,
       checksum, create_options
FROM information_schema.tables
WHERE table_schema = currentDatabase() AND table_name = 'test_infoschema_compat';

SELECT '-- columns: MySQL-specific columns (lowercase)';
SELECT column_name, column_key, privileges, generation_expression, srs_id
FROM information_schema.columns
WHERE table_schema = currentDatabase() AND table_name = 'test_infoschema_compat'
ORDER BY column_name;

SELECT '-- columns: MySQL-specific columns (uppercase)';
SELECT COLUMN_NAME, COLUMN_KEY, PRIVILEGES, GENERATION_EXPRESSION, SRS_ID
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = currentDatabase() AND TABLE_NAME = 'test_infoschema_compat'
ORDER BY COLUMN_NAME;

SELECT '-- collations: the collation advertised in SCHEMATA/TABLES is discoverable';
SELECT COLLATION_NAME, CHARACTER_SET_NAME, ID, IS_DEFAULT, IS_COMPILED, SORTLEN, PAD_ATTRIBUTE
FROM INFORMATION_SCHEMA.COLLATIONS
WHERE COLLATION_NAME = 'utf8mb4_0900_ai_ci';

SELECT '-- collations: lowercase aliases resolve too';
SELECT collation_name, character_set_name, id, is_default, is_compiled, sortlen, pad_attribute
FROM information_schema.collations
WHERE collation_name = 'utf8mb4_0900_ai_ci';

DROP TABLE test_infoschema_compat;
