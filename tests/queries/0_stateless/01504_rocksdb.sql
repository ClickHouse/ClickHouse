DROP TABLE IF EXISTS test;

CREATE TABLE test (key String, value UInt32) Engine=EmbeddedRocksDB; -- { serverError 36 }
CREATE TABLE test (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key2); -- { serverError 47 }
CREATE TABLE test (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key, value); -- { serverError 36 }
CREATE TABLE test (key Tuple(String, UInt32), value UInt64) Engine=EmbeddedRocksDB PRIMARY KEY(key);

DROP TABLE IF EXISTS test;
CREATE TABLE test (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO test SELECT '1_1', number FROM numbers(10000);
SELECT COUNT(1) == 1 FROM test;

INSERT INTO test SELECT concat(toString(number), '_1'), number FROM numbers(10000);
SELECT COUNT(1) == 10000 FROM test;
SELECT uniqExact(key) == 32 FROM (SELECT * FROM test LIMIT 32 SETTINGS max_block_size = 1);
SELECT SUM(value) == 1 + 99 + 900 FROM test WHERE key IN ('1_1', '99_1', '900_1');


DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_memory;

CREATE TABLE test (k UInt32, value UInt64, dummy Tuple(UInt32, Float64), bm AggregateFunction(groupBitmap, UInt64)) Engine=EmbeddedRocksDB PRIMARY KEY(k);
CREATE TABLE test_memory AS test Engine = Memory;

INSERT INTO test SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000000) group by k;

INSERT INTO test_memory SELECT number % 77 AS k, SUM(number) AS value, (1, 1.2), bitmapBuild(groupArray(number)) FROM numbers(10000000) group by k;


SELECT  A.a = B.a, A.b = B.b, A.c = B.c, A.d = B.d, A.e = B.e FROM ( SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM test) A  ANY LEFT JOIN  (SELECT 0 AS a, groupBitmapMerge(bm) AS b , SUM(k) AS c, SUM(value) AS d, SUM(dummy.1) AS e FROM test_memory) B USING a ORDER BY a;

CREATE TEMPORARY TABLE keys AS SELECT * FROM numbers(1000);

SET max_rows_to_read = 2;
SELECT dummy == (1,1.2) FROM test WHERE k IN (1, 3) OR k IN (1) OR k IN (3, 1) OR k IN [1] OR k IN [1, 3] ;
SELECT k == 4 FROM test WHERE k = 4 OR k IN [4] OR k in (4, 10000001, 10000002) AND value > 0;
SELECT k == 4 FROM test WHERE k IN (SELECT toUInt32(number) FROM keys WHERE number = 4);
SELECT k, value FROM test WHERE k = 0 OR value > 0; -- { serverError 158 }
SELECT k, value FROM test WHERE k = 0 AND k IN (1, 3) OR k > 8; -- { serverError 158 }

TRUNCATE TABLE test;
SELECT 0 == COUNT(1) FROM test;

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_memory;

