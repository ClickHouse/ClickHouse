USE test;

DROP TABLE IF EXISTS codecTest;

CREATE TABLE codecTest (
    key      UInt64,
    ref_valueF64 Float64,
    ref_valueF32 Float32,
    valueF64 Float64  CODEC(Gorilla),
    valueF32 Float32  CODEC(Gorilla)
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO codecTest (key, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO codecTest (key, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO codecTest (key, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, n*sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - random values
INSERT INTO codecTest (key, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, (rand64() - 9223372036854775808)/10000000000000 AS v, v, v, v FROM system.numbers LIMIT 3001, 100;


-- These floating-point values are expected to be BINARY equal, hence comparing the values are safe.

SELECT 'F64';
SELECT
	key,
	ref_valueF64, valueF64, ref_valueF64 - valueF64 AS dF64
FROM codecTest
WHERE
	dF64 != 0
LIMIT 10;


SELECT 'F32';
SELECT
	key,
	ref_valueF32, valueF32, ref_valueF32 - valueF32 AS dF32
FROM codecTest
WHERE
	dF32 != 0
LIMIT 10;

DROP TABLE IF EXISTS codecTest;