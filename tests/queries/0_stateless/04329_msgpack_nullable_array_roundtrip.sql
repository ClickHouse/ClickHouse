-- Tags: no-fasttest
-- no-fasttest: MsgPack format not available in fasttest environment

SET allow_experimental_nullable_array_type = 1;
SET engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS test_nullable_array_msgpack;

CREATE TABLE test_nullable_array_msgpack (a Nullable(Array(UInt8))) ENGINE = Memory;

INSERT INTO test_nullable_array_msgpack VALUES (NULL), ([]), ([1, 2]);

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_04329.msgpack', 'MsgPack', 'a Nullable(Array(UInt8))')
SELECT a FROM test_nullable_array_msgpack;

SELECT throwIf(count() != 3)
FROM file(currentDatabase() || '_04329.msgpack', 'MsgPack', 'a Nullable(Array(UInt8))')
FORMAT Null;

SELECT throwIf(countIf(isNull(a)) != 1)
FROM file(currentDatabase() || '_04329.msgpack', 'MsgPack', 'a Nullable(Array(UInt8))')
FORMAT Null;

SELECT throwIf(countIf(not isNull(a) AND empty(a)) != 1)
FROM file(currentDatabase() || '_04329.msgpack', 'MsgPack', 'a Nullable(Array(UInt8))')
FORMAT Null;

SELECT throwIf(countIf(a = [1, 2]) != 1)
FROM file(currentDatabase() || '_04329.msgpack', 'MsgPack', 'a Nullable(Array(UInt8))')
FORMAT Null;

DROP TABLE test_nullable_array_msgpack;
