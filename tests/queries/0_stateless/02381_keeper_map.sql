-- Tags: no-ordinary-database, no-fasttest
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

SET database_atomic_wait_for_drop_and_detach_synchronously = 1;

DROP TABLE IF EXISTS 02381_test;

CREATE TABLE 02381_test (key String, value UInt32) Engine=KeeperMap('/test2381'); -- { serverError 36 }
CREATE TABLE 02381_test (key String, value UInt32) Engine=KeeperMap('/test2381') PRIMARY KEY(key2); -- { serverError 47 }
CREATE TABLE 02381_test (key String, value UInt32) Engine=KeeperMap('/test2381') PRIMARY KEY(key, value); -- { serverError 36 }
CREATE TABLE 02381_test (key Tuple(String, UInt32), value UInt64) Engine=KeeperMap('/test2381') PRIMARY KEY(key);

DROP TABLE IF EXISTS 02381_test;
CREATE TABLE 02381_test (key String, value UInt32) Engine=KeeperMap('/test2381') PRIMARY KEY(key);

INSERT INTO 02381_test SELECT '1_1', number FROM numbers(10000);
SELECT COUNT(1) == 1 FROM 02381_test;

INSERT INTO 02381_test SELECT concat(toString(number), '_1'), number FROM numbers(10000);
SELECT COUNT(1) == 10000 FROM 02381_test;
SELECT uniqExact(key) == 32 FROM (SELECT * FROM 02381_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT SUM(value) == 1 + 99 + 900 FROM 02381_test WHERE key IN ('1_1', '99_1', '900_1');

DROP TABLE IF EXISTS 02381_test;
DROP TABLE IF EXISTS 02381_test_memory;

CREATE TABLE 02381_test (k UInt32, value UInt64, dummy Tuple(UInt32, Float64), bm AggregateFunction(groupBitmap, UInt64)) Engine=KeeperMap('/test2381') PRIMARY KEY(k);
CREATE TABLE 02381_test_memory AS 02381_test Engine = Memory;

INSERT INTO 02381_test SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000000) group by k;

INSERT INTO 02381_test_memory SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000000) group by k;


SELECT  A.a = B.a, A.b = B.b, A.c = B.c, A.d = B.d, A.e = B.e FROM ( SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM 02381_test) A  ANY LEFT JOIN  (SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM 02381_test_memory) B USING a ORDER BY a;

TRUNCATE TABLE 02381_test;
SELECT 0 == COUNT(1) FROM 02381_test;

DROP TABLE IF EXISTS 02381_test;
DROP TABLE IF EXISTS 02381_test_memory;
