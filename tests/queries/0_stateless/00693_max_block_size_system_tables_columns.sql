-- Tags: memory-engine

-- The count() comparisons are scoped to currentDatabase(): concurrent tests create and drop
-- tables between the two subquery executions, so server-wide counts are not stable. The test
-- creates its own fixed set of tables so the counts are non-trivial.

SELECT avg(blockSize()) <= 10 FROM system.tables SETTINGS max_block_size = 10;
SELECT avg(blockSize()) <= 10 FROM system.tables LIMIT 10 SETTINGS max_block_size = 10;

CREATE TABLE t01_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t02_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t03_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t04_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t05_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t06_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t07_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t08_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t09_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t10_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t11_00693 (a UInt8, b String, c Date) ENGINE = Memory;
CREATE TABLE t12_00693 (a UInt8, b String, c Date) ENGINE = Memory;

SELECT (SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS max_block_size = 10) = (SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS max_block_size = 9);
SELECT (SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS max_block_size = 100) = (SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS max_block_size = 1000);

DROP TEMPORARY TABLE IF EXISTS t_00693;
CREATE TEMPORARY TABLE t_00693 (x UInt8);
SELECT database, name, engine, is_temporary, data_paths, metadata_path, toTimeZone(metadata_modification_time, 'UTC'), dependencies_database,
       dependencies_table, create_table_query, engine_full, partition_key, sorting_key, primary_key, sampling_key, storage_policy,
       total_rows, total_bytes FROM system.tables WHERE is_temporary AND name='t_00693';

SELECT avg(blockSize()) <= 10000 FROM system.columns SETTINGS max_block_size = 10;
SELECT avg(blockSize()) <= 10000 FROM system.columns LIMIT 10 SETTINGS max_block_size = 10;
SELECT (SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS max_block_size = 10) = (SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS max_block_size = 9);
SELECT (SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS max_block_size = 100) = (SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS max_block_size = 1000);
SELECT (SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS max_block_size = 13) = (SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS max_block_size = 1000000);
