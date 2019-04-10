DROP TABLE IF EXISTS test.numbers_memory;
CREATE TABLE test.numbers_memory AS system.numbers ENGINE = Memory;
INSERT INTO test.numbers_memory SELECT number FROM system.numbers LIMIT 100;
SELECT DISTINCT number FROM remote('127.0.0.{2,3}', test.numbers_memory) ORDER BY number LIMIT 10;
DROP TABLE test.numbers_memory;
