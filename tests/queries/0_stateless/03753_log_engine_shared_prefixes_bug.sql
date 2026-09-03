DROP TABLE IF EXISTS test;
CREATE TABLE test (c String) ENGINE = Log;
INSERT INTO TABLE test SELECT randomString(10) from numbers(1000);
INSERT INTO TABLE test SELECT randomString(10) from numbers(1000);
SELECT * FROM test ORDER BY c, c.size FORMAT Null;
DROP TABLE test;

