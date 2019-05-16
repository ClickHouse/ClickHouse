DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt64, d32 Decimal32(2), d64 Decimal64(2), d128 Decimal128(2)) ENGINE = Memory;

INSERT INTO test FORMAT CSV "1","1","1","1"
;
INSERT INTO test FORMAT CSV "2","-1","-1","-1"
;
INSERT INTO test FORMAT CSV "3","1.0","1.0","1.0"
;
INSERT INTO test FORMAT CSV "4","-0.1","-0.1","-0.1"
;
INSERT INTO test FORMAT CSV "5","0.010","0.010","0.010"
;

SELECT * FROM test ORDER BY key;

DROP TABLE test;
