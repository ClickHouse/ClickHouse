set enable_debug_queries = 1;

DROP TABLE IF EXISTS a;
CREATE TABLE a (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;

ANALYZE SELECT * FROM a;

DROP TABLE a;
