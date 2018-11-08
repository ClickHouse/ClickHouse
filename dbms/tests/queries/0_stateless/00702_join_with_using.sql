DROP TABLE IF EXISTS test.using1;
DROP TABLE IF EXISTS test.using2;

CREATE TABLE test.using1(a UInt8, b UInt8) ENGINE=Memory;
CREATE TABLE test.using2(a UInt8, b UInt8) ENGINE=Memory;

INSERT INTO test.using1 VALUES (1, 1) (2, 2) (3, 3);
INSERT INTO test.using2 VALUES (4, 4) (2, 2) (3, 3);

SELECT * FROM test.using1 ALL LEFT JOIN (SELECT * FROM test.using2) USING (a, a, a, b, b, b, a, a) ORDER BY a;

DROP TABLE test.using1;
DROP TABLE test.using2;
