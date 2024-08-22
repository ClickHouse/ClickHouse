-- Tags: no-ordinary-database, no-fasttest
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

DROP TABLE IF EXISTS 01686_test;

CREATE TABLE 01686_test (key UInt64, value String) Engine=EmbeddedRocksDB PRIMARY KEY(key) SETTINGS optimize_for_bulk_insert = 0;

SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '01686_test' and name = 'number.keys.written';
INSERT INTO 01686_test SELECT number, format('Hello, world ({})', toString(number)) FROM numbers(10000);
SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '01686_test' and name = 'number.keys.written';

SELECT * FROM 01686_test WHERE key = 123;
SELECT '--';
SELECT * FROM 01686_test WHERE key = -123;
SELECT '--';
SELECT * FROM 01686_test WHERE key = 123 OR key = 4567 ORDER BY key;
SELECT '--';
SELECT * FROM 01686_test WHERE key = NULL;
SELECT '--';
SELECT * FROM 01686_test WHERE key = NULL OR key = 0;
SELECT '--';
SELECT * FROM 01686_test WHERE key IN (123, 456, -123) ORDER BY key;
SELECT '--';
SELECT * FROM 01686_test WHERE key = 'Hello'; -- { serverError TYPE_MISMATCH }

DETACH TABLE 01686_test SYNC;
ATTACH TABLE 01686_test;

SELECT * FROM 01686_test WHERE key IN (99, 999, 9999, -123) ORDER BY key;

DROP TABLE IF EXISTS 01686_test;
