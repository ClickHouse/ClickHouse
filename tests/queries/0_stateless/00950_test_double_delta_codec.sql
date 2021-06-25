DROP TABLE IF EXISTS codecTest;

CREATE TABLE codecTest (
    key      UInt64,
    ref_valueU64 UInt64,
    ref_valueU32 UInt32,
    ref_valueU16 UInt16,
    ref_valueU8  UInt8,
    ref_valueI64 Int64,
    ref_valueI32 Int32,
    ref_valueI16 Int16,
    ref_valueI8  Int8,
    ref_valueDT  DateTime,
    ref_valueD   Date,
    valueU64 UInt64   CODEC(DoubleDelta),
    valueU32 UInt32   CODEC(DoubleDelta),
    valueU16 UInt16   CODEC(DoubleDelta),
    valueU8  UInt8    CODEC(DoubleDelta),
    valueI64 Int64    CODEC(DoubleDelta),
    valueI32 Int32    CODEC(DoubleDelta),
    valueI16 Int16    CODEC(DoubleDelta),
    valueI8  Int8     CODEC(DoubleDelta),
    valueDT  DateTime CODEC(DoubleDelta),
    valueD   Date     CODEC(DoubleDelta)
) Engine = MergeTree ORDER BY key;


-- checking for overflow
INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueI64, valueI64)
	VALUES (1, 18446744073709551615, 18446744073709551615, 9223372036854775807, 9223372036854775807), (2, 0, 0, -9223372036854775808, -9223372036854775808), (3, 18446744073709551615, 18446744073709551615, 9223372036854775807, 9223372036854775807);

-- n^3 covers all double delta storage cases, from small difference between neighbouref_values (stride) to big.
INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16, ref_valueU8, valueU8, ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16, ref_valueI8, valueI8, ref_valueDT, valueDT, ref_valueD, valueD)
	SELECT number as n, n * n * n as v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, toDateTime(v), toDateTime(v), toDate(v), toDate(v)
	FROM system.numbers LIMIT 101, 1000;

-- best case - constant stride
INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16, ref_valueU8, valueU8, ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16, ref_valueI8, valueI8, ref_valueDT, valueDT, ref_valueD, valueD)
	SELECT number as n, n as v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, toDateTime(v), toDateTime(v), toDate(v), toDate(v)
	FROM system.numbers LIMIT 2001, 1000;


-- worst case - random stride
INSERT INTO codecTest (key, ref_valueU64, valueU64, ref_valueU32, valueU32, ref_valueU16, valueU16, ref_valueU8, valueU8, ref_valueI64, valueI64, ref_valueI32, valueI32, ref_valueI16, valueI16, ref_valueI8, valueI8, ref_valueDT, valueDT, ref_valueD, valueD)
	SELECT number as n, n + (rand64() - 9223372036854775807)/1000 as v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, toDateTime(v), toDateTime(v), toDate(v), toDate(v)
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


SELECT 'U16';
SELECT
	key,
	ref_valueU16, valueU16, ref_valueU16 - valueU16 as dU16
FROM codecTest
WHERE
	dU16 != 0
LIMIT 10;


SELECT 'U8';
SELECT
	key,
	ref_valueU8, valueU8, ref_valueU8 - valueU8 as dU8
FROM codecTest
WHERE
	dU8 != 0
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


SELECT 'I16';
SELECT
	key,
	ref_valueI16, valueI16, ref_valueI16 - valueI16 as dI16
FROM codecTest
WHERE
	dI16 != 0
LIMIT 10;


SELECT 'I8';
SELECT
	key,
	ref_valueI8, valueI8, ref_valueI8 - valueI8 as dI8
FROM codecTest
WHERE
	dI8 != 0
LIMIT 10;


SELECT 'DT';
SELECT
	key,
	ref_valueDT, valueDT, ref_valueDT - valueDT as dDT
FROM codecTest
WHERE
	dDT != 0
LIMIT 10;


SELECT 'D';
SELECT
	key,
	ref_valueD, valueD, ref_valueD - valueD as dD
FROM codecTest
WHERE
	dD != 0
LIMIT 10;

SELECT 'Compression:';
SELECT
	table, name, type,
	compression_codec,
	data_uncompressed_bytes u,
	data_compressed_bytes c,
	round(u/c,3) ratio
FROM system.columns
WHERE
	table == 'codecTest'
AND
	compression_codec != ''
AND
	ratio <= 1
ORDER BY
	table, name, type;

DROP TABLE IF EXISTS codecTest;
