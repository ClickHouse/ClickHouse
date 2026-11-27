-- Verify the MySQL-compatibility columns exposed by INFORMATION_SCHEMA views,
-- so that MySQL-aware clients (JDBC/ODBC drivers, IDEs, BI tools) can issue their
-- MySQL-flavored catalog queries without hitting UNKNOWN_IDENTIFIER.

DROP TABLE IF EXISTS test_infoschema_compat;
DROP VIEW IF EXISTS test_infoschema_compat_view;

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

CREATE VIEW test_infoschema_compat_view AS SELECT id FROM test_infoschema_compat;

SELECT '-- tables: ENGINE is NULL for views, as in MySQL';
SELECT table_type, engine, ENGINE
FROM information_schema.tables
WHERE table_schema = currentDatabase() AND table_name = 'test_infoschema_compat_view';

SELECT '-- tables: ENGINE is NULL for system views too';
SELECT table_type, engine
FROM information_schema.tables
WHERE table_schema = 'system' AND table_name = 'one';

DROP TABLE IF EXISTS test_infoschema_compat_memory;
CREATE TABLE test_infoschema_compat_memory (id UInt32) ENGINE = Memory;

SELECT '-- tables: ENGINE is kept for real table engines that do not store data on disk';
SELECT table_type, engine, ENGINE
FROM information_schema.tables
WHERE table_schema = currentDatabase() AND table_name = 'test_infoschema_compat_memory';

DROP TABLE test_infoschema_compat_memory;

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

SELECT '-- collations: the binary pseudo-collation stamped on non-string columns is discoverable';
SELECT COLLATION_NAME, CHARACTER_SET_NAME, ID, IS_DEFAULT, IS_COMPILED, SORTLEN, PAD_ATTRIBUTE
FROM INFORMATION_SCHEMA.COLLATIONS
WHERE COLLATION_NAME = 'binary';

DROP VIEW test_infoschema_compat_view;
DROP TABLE test_infoschema_compat;
