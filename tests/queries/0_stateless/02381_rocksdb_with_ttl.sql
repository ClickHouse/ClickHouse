DROP TABLE IF EXISTS 02381_test;

CREATE TABLE 02381_test (key UInt64, value String) Engine=EmbeddedRocksDB(10) PRIMARY KEY(key);

SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '02381_test' and name = 'number.keys.written';
INSERT INTO 02381_test SELECT number, format('Hello, world ({})', toString(number)) FROM numbers(10000);
SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '02381_test' and name = 'number.keys.written';

SELECT * FROM 02381_test WHERE key = 123;
SELECT '--';
SELECT * FROM 02381_test WHERE key = -123;
SELECT '--';
SELECT * FROM 02381_test WHERE key = 123 OR key = 4567 ORDER BY key;
SELECT '--';
SELECT * FROM 02381_test WHERE key = NULL;
SELECT '--';
SELECT * FROM 02381_test WHERE key = NULL OR key = 0;
SELECT '--';
SELECT * FROM 02381_test WHERE key IN (123, 456, -123) ORDER BY key;
SELECT '--';
SELECT * FROM 02381_test WHERE key = 'Hello'; -- { serverError 53 }

DETACH TABLE 02381_test NO DELAY;
ATTACH TABLE 02381_test;

SELECT * FROM 02381_test WHERE key IN (99, 999, 9999, -123) ORDER BY key;

sleep(10)
SELECT '--';
SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '02381_test' and name = 'number.keys.written';

DROP TABLE IF EXISTS 02381_test;
