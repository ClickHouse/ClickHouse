CREATE TABLE IF NOT EXISTS test.empty_tiny_log(A UInt8) Engine = TinyLog;

SELECT A FROM test.empty_tiny_log;

DROP TABLE test.empty_tiny_log;
