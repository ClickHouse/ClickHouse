DROP TABLE IF EXISTS test.bool_test;

CREATE TABLE test.bool_test (value Bool) ENGINE = Memory;

-- value column shoud have type 'Bool'
SHOW CREATE TABLE test.bool_test;

INSERT INTO test.bool_test (value) VALUES ('false'), ('true'), (0), (1);
INSERT INTO test.bool_test (value) FORMAT JSONEachRow {"value":false}{"value":true}{"value":0}{"value":1}

SELECT value FROM test.bool_test;
SELECT value FROM test.bool_test FORMAT JSONEachRow;
SELECT toUInt64(value) FROM test.bool_test;

DROP TABLE IF EXISTS test.bool_test;
