-- Tags: no-fasttest
-- no-fasttest: MsgPack format not available in fasttest enviroment

-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;

SELECT 'empty tuple';
DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (c0 Nullable(Tuple())) ENGINE = Memory;
INSERT INTO test_empty VALUES (()), (NULL), (());
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_empty.msgpack', 'MsgPack', 'c0 Nullable(Tuple())') SELECT c0 FROM test_empty;
SELECT c0 FROM file(currentDatabase() || '_04029_empty.msgpack', 'MsgPack', 'c0 Nullable(Tuple())');

SELECT 'single element';
DROP TABLE IF EXISTS test_single;
CREATE TABLE test_single (c0 Nullable(Tuple(Int32))) ENGINE = Memory;
INSERT INTO test_single VALUES ((1)), (NULL), ((3));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_single.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32))') SELECT c0 FROM test_single;
SELECT c0 FROM file(currentDatabase() || '_04029_single.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32))');

SELECT 'multiple elements';
DROP TABLE IF EXISTS test_multi;
CREATE TABLE test_multi (c0 Nullable(Tuple(Int32, String, Int64))) ENGINE = Memory;
INSERT INTO test_multi VALUES ((1, 'hello', 314)), (NULL), ((42, 'world', 272));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_multi.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32, String, Int64))') SELECT c0 FROM test_multi;
SELECT c0 FROM file(currentDatabase() || '_04029_multi.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32, String, Int64))');

SELECT 'nested tuple';
DROP TABLE IF EXISTS test_nested;
CREATE TABLE test_nested (c0 Nullable(Tuple(Int32, Tuple(String, Int32)))) ENGINE = Memory;
INSERT INTO test_nested VALUES ((1, ('a', 10))), (NULL), ((2, ('b', 20)));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_nested.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32, Tuple(String, Int32)))') SELECT c0 FROM test_nested;
SELECT c0 FROM file(currentDatabase() || '_04029_nested.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32, Tuple(String, Int32)))');

SELECT 'all nulls';
DROP TABLE IF EXISTS test_allnull;
CREATE TABLE test_allnull (c0 Nullable(Tuple(Int32))) ENGINE = Memory;
INSERT INTO test_allnull VALUES (NULL), (NULL), (NULL);
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_allnull.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32))') SELECT c0 FROM test_allnull;
SELECT c0 FROM file(currentDatabase() || '_04029_allnull.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32))');

SELECT 'nested nullable tuple';
DROP TABLE IF EXISTS test_nested_nullable;
CREATE TABLE test_nested_nullable (c0 Nullable(Tuple(Int32, Nullable(Tuple(String, Int32))))) ENGINE = Memory;
INSERT INTO test_nested_nullable VALUES ((1, ('a', 10))), (NULL), ((2, NULL)), ((3, ('b', 30)));
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_nested_nullable.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32, Nullable(Tuple(String, Int32))))') SELECT c0 FROM test_nested_nullable;
SELECT c0 FROM file(currentDatabase() || '_04029_nested_nullable.msgpack', 'MsgPack', 'c0 Nullable(Tuple(Int32, Nullable(Tuple(String, Int32))))');

SELECT 'multiple nullable tuple columns';
DROP TABLE IF EXISTS test_multicol;
CREATE TABLE test_multicol (a Nullable(Tuple(Int32)), b Nullable(Tuple(String, String))) ENGINE = Memory;
INSERT INTO test_multicol VALUES ((1), ('x', 'y')), (NULL, ('a', 'b')), ((3), NULL);
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04029_multicol.msgpack', 'MsgPack', 'a Nullable(Tuple(Int32)), b Nullable(Tuple(String, String))') SELECT a, b FROM test_multicol;
SELECT a, b FROM file(currentDatabase() || '_04029_multicol.msgpack', 'MsgPack', 'a Nullable(Tuple(Int32)), b Nullable(Tuple(String, String))');
