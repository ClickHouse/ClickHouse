-- Tags: log-engine
DROP TABLE IF EXISTS test_log;
DROP TABLE IF EXISTS test_tiny_log;

CREATE TABLE test_log (x UInt8, s String, a Array(Nullable(String))) ENGINE = Log;
CREATE TABLE test_tiny_log (x UInt8, s String, a Array(Nullable(String))) ENGINE = TinyLog;

INSERT INTO test_log VALUES (64, 'Value1', ['Value2', 'Value3', NULL]);
INSERT INTO test_tiny_log VALUES (64, 'Value1', ['Value2', 'Value3', NULL]);

SELECT data_compressed_bytes FROM system.columns WHERE table = 'test_log' AND database = currentDatabase();
SELECT data_compressed_bytes FROM system.columns WHERE table = 'test_tiny_log' AND database = currentDatabase();

DROP TABLE test_log;
DROP TABLE test_tiny_log;
