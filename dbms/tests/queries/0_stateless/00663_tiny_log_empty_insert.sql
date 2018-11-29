DROP TABLE IF EXISTS test.empty;
DROP TABLE IF EXISTS test.data;

CREATE TABLE test.empty (value Int8) ENGINE = TinyLog;
CREATE TABLE test.data (value Int8) ENGINE = TinyLog;

INSERT INTO test.data SELECT * FROM test.empty;
SELECT * FROM test.data;

INSERT INTO test.data SELECT 1;
SELECT * FROM test.data;

DROP TABLE test.empty;
DROP TABLE test.data;
