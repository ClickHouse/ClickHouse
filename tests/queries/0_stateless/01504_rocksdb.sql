-- Tags: no-ordinary-database, no-fasttest
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

DROP TABLE IF EXISTS 01504_test;

CREATE TABLE 01504_test (key String, value UInt32) Engine=EmbeddedRocksDB; -- { serverError 36 }
CREATE TABLE 01504_test (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key2); -- { serverError 47 }
CREATE TABLE 01504_test (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key, value); -- { serverError 36 }
CREATE TABLE 01504_test (key Tuple(String, UInt32), value UInt64) Engine=EmbeddedRocksDB PRIMARY KEY(key);

DROP TABLE IF EXISTS 01504_test;
CREATE TABLE 01504_test (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO 01504_test SELECT '1_1', number FROM numbers(10000);
SELECT COUNT(1) == 1 FROM 01504_test;

INSERT INTO 01504_test SELECT concat(toString(number), '_1'), number FROM numbers(10000);
SELECT COUNT(1) == 10000 FROM 01504_test;
SELECT uniqExact(key) == 32 FROM (SELECT * FROM 01504_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT SUM(value) == 1 + 99 + 900 FROM 01504_test WHERE key IN ('1_1', '99_1', '900_1');


DROP TABLE IF EXISTS 01504_test;
DROP TABLE IF EXISTS 01504_test_memory;

CREATE TABLE 01504_test (k UInt32, value UInt64, dummy Tuple(UInt32, Float64), bm AggregateFunction(groupBitmap, UInt64)) Engine=EmbeddedRocksDB PRIMARY KEY(k);
CREATE TABLE 01504_test_memory AS 01504_test Engine = Memory;

INSERT INTO 01504_test SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000000) group by k;

INSERT INTO 01504_test_memory SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000000) group by k;


SELECT  A.a = B.a, A.b = B.b, A.c = B.c, A.d = B.d, A.e = B.e FROM ( SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM 01504_test) A  ANY LEFT JOIN  (SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM 01504_test_memory) B USING a ORDER BY a;

CREATE TEMPORARY TABLE keys AS SELECT * FROM system.numbers LIMIT 1 OFFSET 4;

SET max_rows_to_read = 2;
SELECT dummy == (1,1.2) FROM 01504_test WHERE k IN (1, 3) OR k IN (1) OR k IN (3, 1) OR k IN [1] OR k IN [1, 3] ;
SELECT k == 4 FROM 01504_test WHERE k = 4 OR k IN [4] OR k in (4, 10000001, 10000002) AND value > 0;
SELECT k == 4 FROM 01504_test WHERE k IN (SELECT toUInt32(number) FROM keys WHERE number = 4);
SELECT k, value FROM 01504_test WHERE k = 0 OR value > 0; -- { serverError 158 }
SELECT k, value FROM 01504_test WHERE k = 0 AND k IN (1, 3) OR k > 8; -- { serverError 158 }

TRUNCATE TABLE 01504_test;
SELECT 0 == COUNT(1) FROM 01504_test;

DROP TABLE IF EXISTS 01504_test;
DROP TABLE IF EXISTS 01504_test_memory;
