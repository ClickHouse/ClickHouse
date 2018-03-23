SET max_memory_usage = 100000000;
SET max_bytes_before_external_sort = 10000000;

DROP TABLE IF EXISTS test.numbers10m;
CREATE VIEW test.numbers10m AS SELECT number FROM system.numbers LIMIT 10000000;

SELECT number FROM remote('127.0.0.{2,3}', test, numbers10m) ORDER BY number * 1234567890123456789 LIMIT 19999980, 20;

DROP TABLE test.numbers10m;
