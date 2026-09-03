-- Tags: no-fasttest
-- no-fasttest: Avro format is not available in fasttest builds

-- Schema inference must not return Nullable(Tuple) for an Avro union [null, record] unless
-- the Nullable(Tuple) type is allowed by allow_experimental_nullable_tuple_type, because otherwise
-- DESCRIBE would return a type that CREATE TABLE rejects. A null record value is read as the
-- default tuple value (input_format_null_as_default), as it worked before Nullable(Tuple)
-- was supported.

-- { echo }

SET engine_file_truncate_on_insert = 1;

-- Write a file containing a nullable record column and a nullable record with a nested record, while the type is allowed.
SET allow_experimental_nullable_tuple_type = 1;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04494.avro', 'Avro', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') SELECT number, if(number = 1, NULL, (number * 10, 'x')), if(number = 2, NULL, (number * 100, (number * 1000, 'y'))) FROM numbers(3);

-- The default settings must not infer Nullable(Tuple), and CREATE TABLE from the inferred schema must work.
SET allow_experimental_nullable_tuple_type = DEFAULT;
DESCRIBE file(currentDatabase() || '_04494.avro', 'Avro');
SELECT * FROM file(currentDatabase() || '_04494.avro', 'Avro') ORDER BY id;
CREATE TABLE test_04494 ENGINE = Memory AS SELECT * FROM file(currentDatabase() || '_04494.avro', 'Avro');
SELECT count() FROM test_04494;
DROP TABLE test_04494;

-- With the setting enabled, schema inference still returns Nullable(Tuple).
SET allow_experimental_nullable_tuple_type = 1;
DESCRIBE file(currentDatabase() || '_04494.avro', 'Avro');
SELECT * FROM file(currentDatabase() || '_04494.avro', 'Avro') ORDER BY id;

-- Reading into an explicitly requested Nullable(Tuple) structure still works.
SELECT s FROM file(currentDatabase() || '_04494.avro', 'Avro', 'id Int64, s Nullable(Tuple(a Int64, b String)), n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))') ORDER BY id;
