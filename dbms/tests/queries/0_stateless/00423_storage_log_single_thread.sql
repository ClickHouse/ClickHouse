DROP TABLE IF EXISTS test.log;
CREATE TABLE test.log (s String) ENGINE = Log;

SELECT * FROM test.log LIMIT 1;
SELECT * FROM test.log;

DETACH TABLE test.log;
ATTACH TABLE test.log (s String) ENGINE = Log;

SELECT * FROM test.log;
SELECT * FROM test.log LIMIT 1;

INSERT INTO test.log VALUES ('Hello'), ('World');

SELECT * FROM test.log LIMIT 1;

DETACH TABLE test.log;
ATTACH TABLE test.log (s String) ENGINE = Log;

SELECT * FROM test.log LIMIT 1;
SELECT * FROM test.log;

DETACH TABLE test.log;
ATTACH TABLE test.log (s String) ENGINE = Log;

SELECT * FROM test.log;
SELECT * FROM test.log LIMIT 1;

DROP TABLE test.log;
