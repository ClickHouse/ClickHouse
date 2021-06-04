DROP TABLE IF EXISTS 01686_test;

CREATE TABLE 01686_test (key UInt64, value String) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO 01686_test SELECT number, format('Hello, world ({})', toString(number)) FROM numbers(10000);

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
SELECT * FROM 01686_test WHERE key = 'Hello'; -- { serverError 53 }

DETACH TABLE 01686_test NO DELAY;
ATTACH TABLE 01686_test;

SELECT * FROM 01686_test WHERE key IN (99, 999, 9999, -123) ORDER BY key;

DROP TABLE IF EXISTS 01686_test;
