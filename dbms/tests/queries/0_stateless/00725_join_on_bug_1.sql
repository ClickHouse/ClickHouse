DROP TABLE IF EXISTS test.a1;
DROP TABLE IF EXISTS test.a2;

CREATE TABLE test.a1(a UInt8, b UInt8) ENGINE=Memory;
CREATE TABLE test.a2(a UInt8, b UInt8) ENGINE=Memory;

INSERT INTO test.a1 VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO test.a2 VALUES (1, 2), (1, 3), (1, 4);

SELECT * FROM test.a1 as a left JOIN test.a2 as b on a.a=b.a ORDER BY b SETTINGS join_default_strictness='ANY';

DROP TABLE IF EXISTS test.a1;
DROP TABLE IF EXISTS test.a2;

