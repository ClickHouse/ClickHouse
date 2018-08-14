DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test(val Int64) engine = Memory;

INSERT INTO test.test VALUES (1);
INSERT INTO test.test VALUES (2);
INSERT INTO test.test VALUES (3);
INSERT INTO test.test VALUES (4);
INSERT INTO test.test VALUES (5);
INSERT INTO test.test VALUES (6);
INSERT INTO test.test VALUES (7);
INSERT INTO test.test VALUES (8);
INSERT INTO test.test VALUES (9);

SELECT TOP 2 * FROM test.test ORDER BY val;
SELECT TOP (2) * FROM test.test ORDER BY val;
SELECT * FROM test.test ORDER BY val LIMIT 2 OFFSET 2;
SELECT TOP 2 * FROM test.test ORDER BY val LIMIT 2; -- { clientError 406 }
SELECT * FROM test.test ORDER BY val LIMIT 2,3 OFFSET 2; -- { clientError 62 }

DROP TABLE test.test;
