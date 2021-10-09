SELECT sign(0);
SELECT sign(1);
SELECT sign(-1);

DROP TABLE IF EXISTS test;

CREATE TABLE test(
	n1 Int32,
	n2 UInt32,
	n3 Float32,
	n4 Float64,
	n5 Decimal32(5)
) ENGINE = Memory;

INSERT INTO test VALUES (1, 2, -0.0001, 1.5, 0.5) (-2, 0, 2.5, -4, -5) (4, 5, 5, 0, 7);

SELECT 'sign(Int32)';
SELECT sign(n1) FROM test;

SELECT 'sign(UInt32)';
SELECT sign(n2) FROM test;

SELECT 'sign(Float32)';
SELECT sign(n3) FROM test;

SELECT 'sign(Float64)';
SELECT sign(n4) FROM test;

SELECT 'sign(Decimal32(5))';
SELECT sign(n5) FROM test;

DROP TABLE test;
