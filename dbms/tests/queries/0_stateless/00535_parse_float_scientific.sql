DROP TABLE IF EXISTS test.float;
CREATE TABLE test.float (x Float64) ENGINE = Log;

INSERT INTO test.float VALUES (1e7);
SELECT * FROM test.float;

DROP TABLE test.float;
