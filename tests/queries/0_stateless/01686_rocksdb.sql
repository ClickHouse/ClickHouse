DROP TABLE IF EXISTS test;

CREATE TABLE test (key UInt64, value String) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO test SELECT number, format('Hello, world ({})', toString(number)) FROM numbers(10000);

SELECT * FROM test WHERE key = 123;
SELECT '--';
SELECT * FROM test WHERE key = -123;
SELECT '--';
SELECT * FROM test WHERE key = 123 OR key = 4567 ORDER BY key;
SELECT '--';
SELECT * FROM test WHERE key = NULL;
SELECT '--';
SELECT * FROM test WHERE key = NULL OR key = 0;
SELECT '--';
SELECT * FROM test WHERE key IN (123, 456, -123) ORDER BY key;
SELECT '--';
SELECT * FROM test WHERE key = 'Hello'; -- { serverError 53 }

DETACH TABLE test NO DELAY;
ATTACH TABLE test;

SELECT * FROM test WHERE key IN (99, 999, 9999, -123) ORDER BY key;

DROP TABLE IF EXISTS test;

