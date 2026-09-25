-- Tests for `max_temporary_tables`, `max_temporary_table_memory_usage`,
-- `max_temporary_table_size_bytes_compressed` and `max_temporary_table_size_bytes_uncompressed`.

SELECT 'max_temporary_tables';
SET max_temporary_tables = 2;
CREATE TEMPORARY TABLE tmp1 (x UInt64);
CREATE TEMPORARY TABLE tmp2 (x UInt64);
CREATE TEMPORARY TABLE tmp3 (x UInt64); -- { serverError TOO_MANY_TABLES }
-- These do not increase the number of temporary tables.
CREATE TEMPORARY TABLE IF NOT EXISTS tmp1 (x UInt64);
CREATE OR REPLACE TEMPORARY TABLE tmp2 (y String);
CREATE OR REPLACE TEMPORARY TABLE tmp3 (x UInt64); -- { serverError TOO_MANY_TABLES }
-- The temporary tables built for `GLOBAL IN` and CTEs are not counted.
SELECT count() FROM remote('127.0.0.{1,2}', numbers(10)) WHERE number GLOBAL IN (SELECT number FROM numbers(5));
DROP TEMPORARY TABLE tmp1;
CREATE TEMPORARY TABLE tmp3 (x UInt64);
SELECT name FROM system.tables WHERE is_temporary ORDER BY name;
SET max_temporary_tables = 3;
CREATE TEMPORARY TABLE tmp1 (x UInt64);
DROP TEMPORARY TABLE tmp1;
DROP TEMPORARY TABLE tmp2;
DROP TEMPORARY TABLE tmp3;
SET max_temporary_tables = 0;

SELECT 'max_temporary_table_memory_usage';
SET max_temporary_table_memory_usage = '1Mi';
CREATE TEMPORARY TABLE tmp_memory (x UInt64) ENGINE = Memory;
INSERT INTO tmp_memory SELECT number FROM numbers(1000);
-- With one sink nothing of a rejected insert is added.
INSERT INTO tmp_memory SELECT number FROM numbers(1000000) SETTINGS max_threads = 1, max_insert_threads = 1; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM tmp_memory;
-- Parallel sinks commit independently, but the table never exceeds the limit.
INSERT INTO tmp_memory SELECT number FROM numbers(1000000) SETTINGS max_threads = 4, max_insert_threads = 4; -- { serverError TOO_MANY_BYTES }
SELECT total_bytes <= 1048576 FROM system.tables WHERE is_temporary AND name = 'tmp_memory';
TRUNCATE TABLE tmp_memory;
INSERT INTO tmp_memory SELECT number FROM numbers(1000);
-- The limit is taken from the settings of the `INSERT` query.
INSERT INTO tmp_memory SETTINGS max_temporary_table_memory_usage = 0 SELECT number FROM numbers(1000000);
SELECT count() FROM tmp_memory;
INSERT INTO tmp_memory SELECT 1; -- { serverError TOO_MANY_BYTES }
DROP TEMPORARY TABLE tmp_memory;
-- A temporary table with the default engine.
SET default_temporary_table_engine = 'Memory';
CREATE TEMPORARY TABLE tmp_default AS SELECT number FROM numbers(1000000); -- { serverError TOO_MANY_BYTES }
SELECT engine, total_bytes <= 1048576 FROM system.tables WHERE is_temporary AND name = 'tmp_default';
DROP TEMPORARY TABLE tmp_default;
-- The eviction by `max_rows_to_keep` happens before the check: each insert replaces the previous one.
CREATE TEMPORARY TABLE tmp_evicting (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 50000;
INSERT INTO tmp_evicting SELECT number FROM numbers(50000) SETTINGS max_block_size = 50000, max_insert_block_size = 50000;
INSERT INTO tmp_evicting SELECT number FROM numbers(50000) SETTINGS max_block_size = 50000, max_insert_block_size = 50000;
INSERT INTO tmp_evicting SELECT number FROM numbers(50000) SETTINGS max_block_size = 50000, max_insert_block_size = 50000;
SELECT count() FROM tmp_evicting;
DROP TEMPORARY TABLE tmp_evicting;
-- Regular tables and the temporary tables for `GLOBAL IN` are not limited.
DROP TABLE IF EXISTS regular_memory;
CREATE TABLE regular_memory (x UInt64) ENGINE = Memory;
INSERT INTO regular_memory SELECT number FROM numbers(1000000);
SELECT count() FROM regular_memory;
DROP TABLE regular_memory;
SELECT count() FROM remote('127.0.0.{1,2}', numbers(10)) WHERE number GLOBAL IN (SELECT number FROM numbers(1000000));
SET max_temporary_table_memory_usage = 0;

-- Statistics files count towards the size of a table, so do not let the randomized settings add them.
SELECT 'max_temporary_table_size_bytes_compressed';
SET max_temporary_table_size_bytes_compressed = 100000;
CREATE TEMPORARY TABLE tmp_mt (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO tmp_mt SELECT number FROM numbers(1000);
-- Random data does not compress.
INSERT INTO tmp_mt SELECT rand64() FROM numbers(100000) SETTINGS max_block_size = 1000000, max_insert_block_size = 1000000; -- { serverError TOO_MANY_BYTES }
-- A constant compresses well (not zeros, which may use the sparse serialization).
INSERT INTO tmp_mt SELECT 1 FROM numbers(100000) SETTINGS max_block_size = 1000000, max_insert_block_size = 1000000;
SELECT count() FROM tmp_mt;
DROP TEMPORARY TABLE tmp_mt;
CREATE TEMPORARY TABLE tmp_mt ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '' AS SELECT rand64() AS x FROM numbers(100000) SETTINGS max_block_size = 1000000, max_insert_block_size = 1000000; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM tmp_mt;
DROP TEMPORARY TABLE tmp_mt;
SET max_temporary_table_size_bytes_compressed = 0;

SELECT 'max_temporary_table_size_bytes_uncompressed';
SET max_temporary_table_size_bytes_uncompressed = 100000;
CREATE TEMPORARY TABLE tmp_mt (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO tmp_mt SELECT number FROM numbers(1000);
INSERT INTO tmp_mt SELECT 1 FROM numbers(100000) SETTINGS max_block_size = 1000000, max_insert_block_size = 1000000; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM tmp_mt;
DROP TEMPORARY TABLE tmp_mt;
-- Regular tables are not limited.
DROP TABLE IF EXISTS regular_mt;
CREATE TABLE regular_mt (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO regular_mt SELECT number FROM numbers(1000000) SETTINGS max_temporary_table_size_bytes_compressed = 1000;
SELECT count() FROM regular_mt;
DROP TABLE regular_mt;
