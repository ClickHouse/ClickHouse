-- Tags: no-fasttest
-- no-fasttest: Arrow, ORC and Parquet formats are not available in fasttest builds

-- Schema inference must not return Nullable(Tuple) for an optional (nullable) struct column
-- unless the Nullable(Tuple) type is allowed by allow_experimental_nullable_tuple_type, because otherwise
-- DESCRIBE would return a type that CREATE TABLE rejects. The struct null map is propagated
-- into the tuple elements instead, as it worked before Nullable(Tuple) was supported.
-- The Parquet queries with input_format_parquet_use_native_reader_v3 = 0 exercise the legacy
-- Arrow-based Parquet reader in releases that still have it; in newer releases the setting is
-- obsolete and they run on the native reader.

-- { echo }

SET engine_file_truncate_on_insert = 1;

-- Write files containing optional (nullable) struct columns, including a nested struct, while the type is allowed.
SET allow_experimental_nullable_tuple_type = 1;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04493.arrow', 'Arrow', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') SELECT number, if(number = 1, NULL, (number * 10, 'x')), if(number = 2, NULL, (number * 100, (number * 1000, 'y'))) FROM numbers(3);
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04493.arrowstream', 'ArrowStream', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') SELECT number, if(number = 1, NULL, (number * 10, 'x')), if(number = 2, NULL, (number * 100, (number * 1000, 'y'))) FROM numbers(3);
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04493.orc', 'ORC', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') SELECT number, if(number = 1, NULL, (number * 10, 'x')), if(number = 2, NULL, (number * 100, (number * 1000, 'y'))) FROM numbers(3);
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04493.parquet', 'Parquet', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') SELECT number, if(number = 1, NULL, (number * 10, 'x')), if(number = 2, NULL, (number * 100, (number * 1000, 'y'))) FROM numbers(3);

-- The default settings must not infer Nullable(Tuple), and CREATE TABLE from the inferred schema must work.
SET allow_experimental_nullable_tuple_type = DEFAULT;

-- Arrow
DESCRIBE file(currentDatabase() || '_04493.arrow', 'Arrow');
SELECT * FROM file(currentDatabase() || '_04493.arrow', 'Arrow') ORDER BY id;
CREATE TABLE test_04493 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04493.arrow', 'Arrow');
SELECT count() FROM test_04493;
DROP TABLE test_04493;

-- ArrowStream
DESCRIBE file(currentDatabase() || '_04493.arrowstream', 'ArrowStream');
SELECT * FROM file(currentDatabase() || '_04493.arrowstream', 'ArrowStream') ORDER BY id;
CREATE TABLE test_04493 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04493.arrowstream', 'ArrowStream');
SELECT count() FROM test_04493;
DROP TABLE test_04493;

-- ORC (native reader)
DESCRIBE file(currentDatabase() || '_04493.orc', 'ORC');
SELECT * FROM file(currentDatabase() || '_04493.orc', 'ORC') ORDER BY id;
CREATE TABLE test_04493 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04493.orc', 'ORC');
SELECT count() FROM test_04493;
DROP TABLE test_04493;

-- ORC (legacy Arrow-based reader)
DESCRIBE file(currentDatabase() || '_04493.orc', 'ORC') SETTINGS input_format_orc_use_fast_decoder = 0;
SELECT * FROM file(currentDatabase() || '_04493.orc', 'ORC') ORDER BY id SETTINGS input_format_orc_use_fast_decoder = 0;
CREATE TABLE test_04493 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04493.orc', 'ORC') SETTINGS input_format_orc_use_fast_decoder = 0;
SELECT count() FROM test_04493;
DROP TABLE test_04493;

-- Parquet (native reader)
DESCRIBE file(currentDatabase() || '_04493.parquet', 'Parquet');
SELECT * FROM file(currentDatabase() || '_04493.parquet', 'Parquet') ORDER BY id;
CREATE TABLE test_04493 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04493.parquet', 'Parquet');
SELECT count() FROM test_04493;
DROP TABLE test_04493;

-- Parquet (legacy Arrow-based reader where available)
DESCRIBE file(currentDatabase() || '_04493.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 0;
SELECT * FROM file(currentDatabase() || '_04493.parquet', 'Parquet') ORDER BY id SETTINGS input_format_parquet_use_native_reader_v3 = 0;
CREATE TABLE test_04493 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04493.parquet', 'Parquet') SETTINGS input_format_parquet_use_native_reader_v3 = 0;
SELECT count() FROM test_04493;
DROP TABLE test_04493;

-- With the setting enabled, schema inference still returns Nullable(Tuple).
SET allow_experimental_nullable_tuple_type = 1;
DESCRIBE file(currentDatabase() || '_04493.arrow', 'Arrow');
DESCRIBE file(currentDatabase() || '_04493.arrowstream', 'ArrowStream');
DESCRIBE file(currentDatabase() || '_04493.orc', 'ORC') SETTINGS input_format_orc_use_fast_decoder = 0;
SELECT * FROM file(currentDatabase() || '_04493.arrow', 'Arrow') ORDER BY id;

-- Reading into an explicitly requested Nullable(Tuple) structure still works.
SELECT s FROM file(currentDatabase() || '_04493.arrow', 'Arrow', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') ORDER BY id;
