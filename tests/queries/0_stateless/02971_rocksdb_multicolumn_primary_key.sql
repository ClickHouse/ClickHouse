-- Tags: no-ordinary-database, no-fasttest
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

DROP TABLE IF EXISTS 02971_test;

CREATE TABLE 02971_test (k1 UInt64, k2 UInt64, val UInt64) ENGINE=EmbeddedRocksDB PRIMARY KEY (k1, k2);

INSERT INTO 02971_test SELECT number %10, number, number FROM numbers(100);

SELECT COUNT(1) == 100 FROM 02971_test;    -- Full scan
SELECT COUNT(1) == 5 FROM 02971_test WHERE k1 = 5 AND k2 IN (15, 35, 55, 75, 95);   -- Key scan
SELECT COUNT(1) == 0 FROM 02971_test WHERE k1 = 6 AND k2 IN (15, 35, 55, 75, 95);
SELECT COUNT(1) == 1 FROM 02971_test WHERE k1 IN (1, 3, 5) AND k2 = 15 AND k1 IN (3, 5, 7);
SELECT COUNT(1) == 0 FROM 02971_test WHERE k1 IN (1, 3, 5) AND k2 = 15 AND k1 IN (2, 4, 6);
SELECT COUNT(1) == 6 FROM 02971_test WHERE (k1 IN (1, 3, 5) OR k1 IN (2, 3, 4, 5, 6)) AND k2 IN (11, 13, 15, 12, 14, 16);

DROP TABLE IF EXISTS 02971_test;