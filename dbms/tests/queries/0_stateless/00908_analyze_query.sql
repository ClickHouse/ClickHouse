set enable_debug_queries = 1;

DROP TABLE IF EXISTS test.a;
CREATE TABLE test.a (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;

ANALYZE SELECT * FROM test.a;

DROP TABLE test.a;
