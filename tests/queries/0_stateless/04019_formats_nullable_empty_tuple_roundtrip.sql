-- Tags: no-fasttest
-- no-fasttest: Some formats not available in fasttest enviroment

-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS test_nullable_empty_tuple;
CREATE TABLE test_nullable_empty_tuple (c0 Nullable(Tuple())) ENGINE = Memory;
INSERT INTO TABLE test_nullable_empty_tuple (c0) VALUES (()), (NULL), (());

SELECT 'CSV';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.csv', 'CSV', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.csv', 'CSV', 'c0 Nullable(Tuple())');

SELECT 'TabSeparated';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.tsv', 'TabSeparated', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.tsv', 'TabSeparated', 'c0 Nullable(Tuple())');

SELECT 'TabSeparatedRaw';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.tsvraw', 'TabSeparatedRaw', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.tsvraw', 'TabSeparatedRaw', 'c0 Nullable(Tuple())');

SELECT 'JSONEachRow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.json', 'JSONEachRow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.json', 'JSONEachRow', 'c0 Nullable(Tuple())');

SELECT 'JSONCompactEachRow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsoncompact', 'JSONCompactEachRow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsoncompact', 'JSONCompactEachRow', 'c0 Nullable(Tuple())');

SELECT 'JSONStringsEachRow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsonstr', 'JSONStringsEachRow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsonstr', 'JSONStringsEachRow', 'c0 Nullable(Tuple())');

SELECT 'JSONCompactStringsEachRow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsoncstr', 'JSONCompactStringsEachRow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsoncstr', 'JSONCompactStringsEachRow', 'c0 Nullable(Tuple())');

SELECT 'Values';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.values', 'Values', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.values', 'Values', 'c0 Nullable(Tuple())');

SELECT 'TSKV';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.tskv', 'TSKV', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.tskv', 'TSKV', 'c0 Nullable(Tuple())');

SELECT 'CustomSeparated';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.custom', 'CustomSeparated', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.custom', 'CustomSeparated', 'c0 Nullable(Tuple())');

SELECT 'Native';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.native', 'Native', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.native', 'Native', 'c0 Nullable(Tuple())');

SELECT 'RowBinary';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.rowbin', 'RowBinary', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.rowbin', 'RowBinary', 'c0 Nullable(Tuple())');

SELECT 'Avro';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.avro', 'Avro', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.avro', 'Avro', 'c0 Nullable(Tuple())');

SELECT 'MsgPack';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.msgpack', 'MsgPack', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.msgpack', 'MsgPack', 'c0 Nullable(Tuple())');

SELECT 'BSONEachRow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.bson', 'BSONEachRow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.bson', 'BSONEachRow', 'c0 Nullable(Tuple())');

SELECT 'JSON';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsonall', 'JSON', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsonall', 'JSON', 'c0 Nullable(Tuple())');

SELECT 'JSONCompact';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsoncompactall', 'JSONCompact', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsoncompactall', 'JSONCompact', 'c0 Nullable(Tuple())');

SELECT 'JSONColumns';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsoncols', 'JSONColumns', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsoncols', 'JSONColumns', 'c0 Nullable(Tuple())');

SELECT 'JSONCompactColumns';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsonccols', 'JSONCompactColumns', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsonccols', 'JSONCompactColumns', 'c0 Nullable(Tuple())');

SELECT 'JSONColumnsWithMetadata';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsoncolsmeta', 'JSONColumnsWithMetadata', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsoncolsmeta', 'JSONColumnsWithMetadata', 'c0 Nullable(Tuple())');

SELECT 'JSONObjectEachRow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.jsonobj', 'JSONObjectEachRow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.jsonobj', 'JSONObjectEachRow', 'c0 Nullable(Tuple())');

SELECT 'Buffers';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.buf', 'Buffers', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.buf', 'Buffers', 'c0 Nullable(Tuple())');

-- Parquet doesn't support empty tuples by design
SELECT 'Parquet';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.parquet', 'Parquet', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple; -- { serverError BAD_ARGUMENTS }

-- TODO: Does not work for Arrow, ArrowStream, and ORC but should
SELECT 'Arrow';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.arrow', 'Arrow', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.arrow', 'Arrow', 'c0 Nullable(Tuple())');

SELECT 'ArrowStream';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.arrowstream', 'ArrowStream', 'c0 Nullable(Tuple())');

SELECT 'ORC';
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04019.orc', 'ORC', 'c0 Nullable(Tuple())') SELECT c0 FROM test_nullable_empty_tuple;
SELECT c0 FROM file(currentDatabase() || '_04019.orc', 'ORC', 'c0 Nullable(Tuple())');
