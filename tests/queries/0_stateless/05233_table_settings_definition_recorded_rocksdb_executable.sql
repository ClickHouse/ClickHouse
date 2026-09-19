-- Tags: no-fasttest, use-rocksdb
-- Tag no-fasttest: in fasttest, ENABLE_LIBRARIES=0, so the EmbeddedRocksDB engine is not built.
--
-- `EmbeddedRocksDB` and `Executable` record the table's own `SETTINGS` clause in the settings object as they
-- apply it, so `system.table_settings` reads the source from there rather than from the stored `CREATE`
-- query. The two have to agree wherever the settings can change: on `CREATE`, for `EmbeddedRocksDB` after
-- `ALTER ... MODIFY SETTING` and `RESET SETTING`, which rebuild the settings from the whole clause, and after
-- the table is loaded again from what it stored.
--
-- `Executable` is created with a script that does not exist: the script is looked up when the table is read.

DROP TABLE IF EXISTS rocksdb_definition;
DROP TABLE IF EXISTS executable_definition;

CREATE TABLE rocksdb_definition (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k
SETTINGS bulk_insert_block_size = 1000;

SELECT '-- EmbeddedRocksDB: CREATE';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'rocksdb_definition'
ORDER BY name;

SELECT '-- EmbeddedRocksDB: MODIFY SETTING';
ALTER TABLE rocksdb_definition MODIFY SETTING optimize_for_bulk_insert = 0;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'rocksdb_definition'
ORDER BY name;

SELECT '-- EmbeddedRocksDB: RESET SETTING';
ALTER TABLE rocksdb_definition RESET SETTING bulk_insert_block_size;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'rocksdb_definition'
ORDER BY name;

SELECT '-- EmbeddedRocksDB: loaded again from what it stored';
DETACH TABLE rocksdb_definition;
ATTACH TABLE rocksdb_definition;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'rocksdb_definition'
ORDER BY name;

CREATE TABLE executable_definition (x UInt64) ENGINE = Executable('nonexistent_05233.sh', 'TSV')
SETTINGS send_chunk_header = 1, command_termination_timeout = 3;

SELECT '-- Executable: CREATE';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'executable_definition'
    AND name IN ('send_chunk_header', 'command_termination_timeout', 'pool_size')
ORDER BY name;

SELECT '-- Executable: loaded again from what it stored';
DETACH TABLE executable_definition;
ATTACH TABLE executable_definition;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'executable_definition'
    AND name IN ('send_chunk_header', 'command_termination_timeout', 'pool_size')
ORDER BY name;

DROP TABLE rocksdb_definition;
DROP TABLE executable_definition;
