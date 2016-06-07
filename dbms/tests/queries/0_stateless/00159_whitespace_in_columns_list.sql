DROP TABLE IF EXISTS test.memory;
CREATE TABLE test.memory (x UInt8) ENGINE = Memory;

INSERT INTO test.memory VALUES (1);
INSERT INTO test.memory (x) VALUES (2);
INSERT INTO test.memory ( x) VALUES (3);
INSERT INTO test.memory (x ) VALUES (4);
INSERT INTO test.memory ( x ) VALUES (5);
INSERT INTO test.memory(x)VALUES(6);

SELECT * FROM test.memory ORDER BY x;

DROP TABLE test.memory;
