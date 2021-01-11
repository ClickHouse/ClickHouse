DROP TABLE IF EXISTS memory;
CREATE TABLE memory (x UInt8) ENGINE = Memory;

INSERT INTO memory VALUES (1);
INSERT INTO memory (x) VALUES (2);
INSERT INTO memory ( x) VALUES (3);
INSERT INTO memory (x ) VALUES (4);
INSERT INTO memory ( x ) VALUES (5);
INSERT INTO memory(x)VALUES(6);

SELECT * FROM memory ORDER BY x;

DROP TABLE memory;
