DROP TABLE IF EXISTS 02381_test;

CREATE TABLE 02381_test (key UInt64, value String) Engine=EmbeddedRocksDB("0", "true") PRIMARY KEY(key);

SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '02381_test' and name = 'number.keys.written';
INSERT INTO 02381_test SELECT number, format('Hello, world ({})', toString(number)) FROM numbers(10000);
SELECT value FROM system.rocksdb WHERE database = currentDatabase() and table = '02381_test' and name = 'number.keys.written';

SELECT * FROM 02381_test WHERE key = 123;
SELECT '--';

DROP TABLE IF EXISTS 02381_test;
