DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS random_filename;

CREATE TABLE t0 (c0 Int32, c1 Tuple()) ENGINE = Memory;
CREATE TABLE random_filename (name String) ENGINE = Memory;

INSERT INTO random_filename SELECT concat('03716_test_bson_empty_tuple_', toString(generateUUIDv4()), '.bson');

INSERT INTO FUNCTION file((SELECT name FROM random_filename LIMIT 1), 'BSONEachRow', 'c0 Int32, c1 Tuple()')
SELECT 1, tuple() FROM numbers(5) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t0 SELECT * FROM file((SELECT name FROM random_filename LIMIT 1), 'BSONEachRow', 'c0 Int32, c1 Tuple()');

SELECT * FROM t0 ORDER BY c0;

DROP TABLE t0;
DROP TABLE random_filename;
