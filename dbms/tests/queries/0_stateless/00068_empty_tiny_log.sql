CREATE TABLE IF NOT EXISTS test_empty_tiny_log(A UInt8) Engine = TinyLog;

SELECT A FROM test_empty_tiny_log;

DROP TABLE  test_empty_tiny_log;
