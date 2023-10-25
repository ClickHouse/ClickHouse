DROP TABLE IF EXISTS test_hierarhical_table;

SET allow_table_engine_tinylog=1;

CREATE TABLE IF NOT EXISTS empty_tiny_log(A UInt8) Engine = TinyLog;

SELECT A FROM empty_tiny_log;

DROP TABLE empty_tiny_log;
