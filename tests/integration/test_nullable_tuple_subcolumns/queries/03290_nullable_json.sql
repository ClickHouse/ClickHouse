-- Tuple-related queries from tests/queries/0_stateless/03290_nullable_json.sql.j2.

SET enable_json_type = 1;

DROP TABLE IF EXISTS test;

SELECT '---';
CREATE TABLE test (json Nullable(JSON(a UInt32, b Array(UInt32), c Nullable(UInt32), d Tuple(e UInt32, f Nullable(UInt32))))) ENGINE=Memory;
INSERT INTO test SELECT number % 2 ? NULL : '{"a" : 1, "b" : [1, 2, 3], "c" : null, "d" : {"e" : 1, "f" : null}, "x" : 42, "y" : [1, 2, 3]}' FROM numbers(4);
SELECT json.d AS path, toTypeName(path) FROM test;
SELECT json.d.e AS path, toTypeName(path) FROM test;
SELECT json.d.f AS path, toTypeName(path) FROM test;
DROP TABLE test;

SELECT '---';
CREATE TABLE test (json Nullable(JSON(a UInt32, b Array(UInt32), c Nullable(UInt32), d Tuple(e UInt32, f Nullable(UInt32))))) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000;
INSERT INTO test SELECT number % 2 ? NULL : '{"a" : 1, "b" : [1, 2, 3], "c" : null, "d" : {"e" : 1, "f" : null}, "x" : 42, "y" : [1, 2, 3]}' FROM numbers(4);
SELECT json.d AS path, toTypeName(path) FROM test;
SELECT json.d.e AS path, toTypeName(path) FROM test;
SELECT json.d.f AS path, toTypeName(path) FROM test;
DROP TABLE test;

SELECT '---';
CREATE TABLE test (json Nullable(JSON(a UInt32, b Array(UInt32), c Nullable(UInt32), d Tuple(e UInt32, f Nullable(UInt32))))) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
INSERT INTO test SELECT number % 2 ? NULL : '{"a" : 1, "b" : [1, 2, 3], "c" : null, "d" : {"e" : 1, "f" : null}, "x" : 42, "y" : [1, 2, 3]}' FROM numbers(4);
SELECT json.d AS path, toTypeName(path) FROM test;
SELECT json.d.e AS path, toTypeName(path) FROM test;
SELECT json.d.f AS path, toTypeName(path) FROM test;
DROP TABLE test;
