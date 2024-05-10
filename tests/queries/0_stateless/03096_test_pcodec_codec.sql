--Tags: no-fasttest
-- no-fasttest because Pcodec isn't available in fasttest

DROP TABLE IF EXISTS codecTest;

SET allow_experimental_codecs = 1;
SET cross_to_inner_join_rewrite = 1;

CREATE TABLE codecTest (
    key      UInt64,
    name     String,
    ref_valueF64 Float64,
    ref_valueF32 Float32,
    valueF64 Float64  CODEC(Pcodec),
    valueF32 Float32  CODEC(Pcodec)
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'e()', e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'log2(n)', log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'n*sqrt(n)', n*sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - almost like a random values
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'sin(n*n*n)*n', sin(n * n * n * n* n) AS v, v, v, v FROM system.numbers LIMIT 301, 100;


-- These floating-point values are expected to be BINARY equal, so comparing by-value is Ok here.

-- referencing previous row key, value, and case name to simplify debugging.
SELECT 'F64';
SELECT
	c1.key, c1.name,
	c1.ref_valueF64, c1.valueF64, c1.ref_valueF64 - c1.valueF64 AS dF64,
	'prev:',
	c2.key, c2.ref_valueF64
FROM
	codecTest as c1, codecTest as c2
WHERE
	dF64 != 0
AND
	c2.key = c1.key - 1
LIMIT 10;


SELECT 'F32';
SELECT
	c1.key, c1.name,
	c1.ref_valueF32, c1.valueF32, c1.ref_valueF32 - c1.valueF32 AS dF32,
	'prev:',
	c2.key, c2.ref_valueF32
FROM
	codecTest as c1, codecTest as c2
WHERE
	dF32 != 0
AND
	c2.key = c1.key - 1
LIMIT 10;

DROP TABLE IF EXISTS codecTest;

CREATE TABLE codecTest (
    key      UInt64,
    name     String,
    ref_valueF64 Float64,
    ref_valueF32 Float32,
    valueF64 Float64  CODEC(Pcodec(6)),
    valueF32 Float32  CODEC(Pcodec(6))
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'e()', e() AS v, v, v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'log2(n)', log2(n) AS v, v, v, v FROM system.numbers LIMIT 101, 100;

-- bad case - values differ significantly
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'n*sqrt(n)', n*sqrt(n) AS v, v, v, v FROM system.numbers LIMIT 201, 100;

-- worst case - almost like a random values
INSERT INTO codecTest (key, name, ref_valueF64, valueF64, ref_valueF32, valueF32)
	SELECT number AS n, 'sin(n*n*n)*n', sin(n * n * n * n* n) AS v, v, v, v FROM system.numbers LIMIT 301, 100;


-- These floating-point values are expected to be BINARY equal, so comparing by-value is Ok here.

-- referencing previous row key, value, and case name to simplify debugging.
SELECT 'F64';
SELECT
	c1.key, c1.name,
	c1.ref_valueF64, c1.valueF64, c1.ref_valueF64 - c1.valueF64 AS dF64,
	'prev:',
	c2.key, c2.ref_valueF64
FROM
	codecTest as c1, codecTest as c2
WHERE
	dF64 != 0
AND
	c2.key = c1.key - 1
LIMIT 10;


SELECT 'F32';
SELECT
	c1.key, c1.name,
	c1.ref_valueF32, c1.valueF32, c1.ref_valueF32 - c1.valueF32 AS dF32,
	'prev:',
	c2.key, c2.ref_valueF32
FROM
	codecTest as c1, codecTest as c2
WHERE
	dF32 != 0
AND
	c2.key = c1.key - 1
LIMIT 10;

DROP TABLE IF EXISTS codecTest;

CREATE TABLE codecTest (
    key      UInt64,
    name     String,
    ref_valueU64 UInt64,
	ref_valueU32 UInt32,
	ref_valueI64 Int64,
	ref_valueI32 Int32,
	valueU64 UInt64  CODEC(Pcodec),
	valueU32 UInt32  CODEC(Pcodec),
	valueI64 Int64  CODEC(Pcodec),
	valueI32 Int32  CODEC(Pcodec),
) Engine = MergeTree ORDER BY key;

INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueI64, valueI64, ref_valueI32, valueI32)
    SELECT number as n, n * n * n as v, v, v, v, v, v, v, v
    FROM system.numbers LIMIT 101, 1000;

INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueI64, valueI64, ref_valueI32, valueI32)
    SELECT number as n, n + (rand64() - 9223372036854775807)/1000 as v, v, v, v, v, v, v, v
    FROM system.numbers LIMIT 3001, 1000;

SELECT 'U64';
SELECT
    key,
    ref_valueU64, valueU64, ref_valueU64 - valueU64 as dU64
FROM codecTest
WHERE
    dU64 != 0
LIMIT 10;


SELECT 'U32';
SELECT
    key,
    ref_valueU32, valueU32, ref_valueU32 - valueU32 as dU32
FROM codecTest
WHERE
    dU32 != 0
LIMIT 10;

SELECT 'I64';
SELECT
    key,
    ref_valueI64, valueI64, ref_valueI64 - valueI64 as dI64
FROM codecTest
WHERE
    dI64 != 0
LIMIT 10;


SELECT 'I32';
SELECT
    key,
    ref_valueI32, valueI32, ref_valueI32 - valueI32 as dI32
FROM codecTest
WHERE
    dI32 != 0
LIMIT 10;

DROP TABLE IF EXISTS codecTest;

CREATE TABLE codecTest (
    key      UInt64,
    name     String,
    ref_valueU64 UInt64,
	ref_valueU32 UInt32,
	ref_valueI64 Int64,
	ref_valueI32 Int32,
	valueU64 UInt64  CODEC(Pcodec(11)),
	valueU32 UInt32  CODEC(Pcodec(10)),
	valueI64 Int64  CODEC(Pcodec(2)),
	valueI32 Int32  CODEC(Pcodec(4)),
) Engine = MergeTree ORDER BY key;

INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueI64, valueI64, ref_valueI32, valueI32)
    SELECT number as n, n * n * n as v, v, v, v, v, v, v, v
    FROM system.numbers LIMIT 101, 1000;

INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueI64, valueI64, ref_valueI32, valueI32)
    SELECT number as n, n + (rand64() - 9223372036854775807)/1000 as v, v, v, v, v, v, v, v
    FROM system.numbers LIMIT 3001, 1000;

SELECT 'U64';
SELECT
    key,
    ref_valueU64, valueU64, ref_valueU64 - valueU64 as dU64
FROM codecTest
WHERE
    dU64 != 0
LIMIT 10;


SELECT 'U32';
SELECT
    key,
    ref_valueU32, valueU32, ref_valueU32 - valueU32 as dU32
FROM codecTest
WHERE
    dU32 != 0
LIMIT 10;

SELECT 'I64';
SELECT
    key,
    ref_valueI64, valueI64, ref_valueI64 - valueI64 as dI64
FROM codecTest
WHERE
    dI64 != 0
LIMIT 10;


SELECT 'I32';
SELECT
    key,
    ref_valueI32, valueI32, ref_valueI32 - valueI32 as dI32
FROM codecTest
WHERE
    dI32 != 0
LIMIT 10;

DROP TABLE IF EXISTS codecTest;
