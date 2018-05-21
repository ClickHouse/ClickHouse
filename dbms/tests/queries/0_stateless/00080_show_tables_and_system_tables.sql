DROP DATABASE IF EXISTS test_show_tables;

CREATE DATABASE test_show_tables;

CREATE TABLE test_show_tables.A (A UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_tables.B (A UInt8) ENGINE = TinyLog;

SHOW TABLES from test_show_tables;

SELECT name, toUInt32(metadata_modification_time) > 0, engine_full, create_table_query FROM system.tables WHERE database = 'test_show_tables' ORDER BY name FORMAT TSVRaw;

CREATE TEMPORARY TABLE test_temporary_table (id UInt64);
SELECT name FROM system.tables WHERE is_temporary = 1 AND name = 'test_temporary_table';

DROP DATABASE test_show_tables;


-- Check that create_table_query works for system tables and unusual Databases
DROP DATABASE IF EXISTS test_DatabaseMemory;
CREATE DATABASE test_DatabaseMemory ENGINE = Memory;
CREATE TABLE test_DatabaseMemory.A (A UInt8) ENGINE = Null;

-- Just in case
DROP DATABASE IF EXISTS test_DatabaseDictionary;
CREATE DATABASE test_DatabaseDictionary ENGINE = Dictionary;

SELECT sum(ignore(*, metadata_modification_time, engine_full, create_table_query)) FROM system.tables;

DROP DATABASE test_DatabaseDictionary;
DROP DATABASE test_DatabaseMemory;
