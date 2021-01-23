CREATE TEMPORARY TABLE test ("\\" String DEFAULT '\r\n\t\\' || '
');

INSERT INTO test VALUES ('Hello, world!');
INSERT INTO test ("\\") VALUES ('Hello, world!');

SELECT * FROM test;

DROP TEMPORARY TABLE test;
DROP TABLE IF EXISTS test;

CREATE TABLE test (x UInt64, "\\" String DEFAULT '\r\n\t\\' || '
') ENGINE = MergeTree ORDER BY x;

INSERT INTO test (x) VALUES (1);

SELECT * FROM test;

DROP TABLE test;
