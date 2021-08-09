CREATE TABLE IF NOT EXISTS empty_tiny_log(A UInt8) Engine = TinyLog;

SELECT A FROM empty_tiny_log;

DROP TABLE empty_tiny_log;
