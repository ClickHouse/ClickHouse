DROP TABLE IF EXISTS index_memory;
CREATE TABLE index_memory (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO index_memory SELECT * FROM system.numbers LIMIT 10000000;
SELECT count() FROM index_memory;
DETACH TABLE index_memory;
SET max_memory_usage = 79000000;
ATTACH TABLE index_memory;
SELECT count() FROM index_memory;
DROP TABLE index_memory;
