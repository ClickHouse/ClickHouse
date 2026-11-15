-- Tags: memory-engine
DROP TABLE IF EXISTS t_memory_compressed;

CREATE TABLE t_memory_compressed (id UInt64, s String, arr Array(LowCardinality(String)), m Map(String, String))
ENGINE = Memory SETTINGS compress = 1;

INSERT INTO t_memory_compressed VALUES (1, 'foo', range(5), map('k1', 'v1'));
INSERT INTO t_memory_compressed VALUES (2, 'bar', range(5), map('k2', 'v2'));

SELECT * FROM t_memory_compressed ORDER BY id;

DROP TABLE t_memory_compressed;
