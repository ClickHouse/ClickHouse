DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int32, c1 Tuple()) ENGINE = Memory;

INSERT INTO FUNCTION file('03716_test_bson_empty_tuple.bson', 'BSONEachRow', 'c0 Int32, c1 Tuple()')
SELECT 1, tuple() from numbers(5)
SETTINGS engine_file_truncate_on_insert=1;

INSERT INTO t0 SELECT * FROM file('03716_test_bson_empty_tuple.bson', 'BSONEachRow', 'c0 Int32, c1 Tuple()');

SELECT * FROM t0 ORDER BY c0;
