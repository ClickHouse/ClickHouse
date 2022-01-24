-- Tags: distributed

DROP TABLE IF EXISTS numbers_memory;
CREATE TABLE numbers_memory AS system.numbers ENGINE = Memory;
INSERT INTO numbers_memory SELECT number FROM system.numbers LIMIT 100;
SELECT DISTINCT number FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_memory) ORDER BY number LIMIT 10;
DROP TABLE numbers_memory;
