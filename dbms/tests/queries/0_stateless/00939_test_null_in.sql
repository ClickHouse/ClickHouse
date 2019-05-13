DROP TABLE IF EXISTS test.nullt;

CREATE TABLE test.nullt (c1 Nullable(UInt32), c2 Nullable(String))ENGINE = Log;
INSERT INTO test.nullt VALUES (1, 'abc'), (2, NULL), (NULL, NULL);

SELECT c2 = ('abc') FROM test.nullt;
SELECT c2 IN ('abc') FROM test.nullt;

SELECT c2 IN ('abc', NULL) FROM test.nullt;

DROP TABLE IF EXISTS test.nullt;
