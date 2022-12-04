-- Tags: no-ordinary-database, no-fasttest, long

DROP TABLE IF EXISTS _02416_test SYNC;

CREATE TABLE _02416_test (key String, value UInt32) Engine=KeeperMap('/' || currentDatabase() || '/test2416'); -- { serverError 36 }
CREATE TABLE _02416_test (key String, value UInt32) Engine=KeeperMap('/' || currentDatabase() || '/test2416') PRIMARY KEY(key2); -- { serverError 47 }
CREATE TABLE _02416_test (key String, value UInt32) Engine=KeeperMap('/' || currentDatabase() || '/test2416') PRIMARY KEY(key, value); -- { serverError 36 }
CREATE TABLE _02416_test (key String, value UInt32) Engine=KeeperMap('/' || currentDatabase() || '/test2416') PRIMARY KEY(concat(key, value)); -- { serverError 36 }
CREATE TABLE _02416_test (key Tuple(String, UInt32), value UInt64) Engine=KeeperMap('/' || currentDatabase() || '/test2416') PRIMARY KEY(key);

DROP TABLE IF EXISTS _02416_test SYNC;
CREATE TABLE _02416_test (key String, value UInt32) Engine=KeeperMap('/' || currentDatabase() || '/test2416') PRIMARY KEY(key);

INSERT INTO _02416_test SELECT '1_1', number FROM numbers(1000);
SELECT COUNT(1) == 1 FROM _02416_test;

INSERT INTO _02416_test SELECT concat(toString(number), '_1'), number FROM numbers(1000);
SELECT COUNT(1) == 1000 FROM _02416_test;
SELECT uniqExact(key) == 32 FROM (SELECT * FROM _02416_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT SUM(value) == 1 + 99 + 900 FROM _02416_test WHERE key IN ('1_1', '99_1', '900_1');

DROP TABLE IF EXISTS _02416_test SYNC;
DROP TABLE IF EXISTS _02416_test_memory;

CREATE TABLE _02416_test (k UInt32, value UInt64, dummy Tuple(UInt32, Float64), bm AggregateFunction(groupBitmap, UInt64)) Engine=KeeperMap('/' || currentDatabase() || '/test2416') PRIMARY KEY(k);
CREATE TABLE _02416_test_memory AS _02416_test Engine = Memory;

INSERT INTO _02416_test SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000) group by k;

INSERT INTO _02416_test_memory SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000) group by k;

SELECT  A.a = B.a, A.b = B.b, A.c = B.c, A.d = B.d, A.e = B.e FROM ( SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM _02416_test) A  ANY LEFT JOIN  (SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM _02416_test_memory) B USING a ORDER BY a;

TRUNCATE TABLE _02416_test;
SELECT 0 == COUNT(1) FROM _02416_test;

DROP TABLE IF EXISTS _02416_test SYNC;
DROP TABLE IF EXISTS _02416_test_memory;
