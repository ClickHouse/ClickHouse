DROP TABLE IF EXISTS reference;
DROP TABLE IF EXISTS doubleDelta;

CREATE TABLE reference (
    key      UInt64,
    valueU64 UInt64,
    valueU32 UInt32,
    valueU16 UInt16,
    valueU8  UInt8,
    valueI64 Int64,
    valueI32 Int32,
    valueI16 Int16,
    valueI8  Int8,
    valueDT  DateTime,
    valueD   Date
) Engine = MergeTree ORDER BY key;


CREATE TABLE doubleDelta (
    key      UInt64   CODEC(DoubleDelta),
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


-- n^3 covers all double delta storage cases, from small difference between neighbour values (stride) to big.
INSERT INTO reference (key, valueU64, valueU32, valueU16, valueU8, valueI64, valueI32, valueI16, valueI8, valueDT, valueD)
	SELECT number as n, n * n * n as v, v, v, v, v, v, v, v, toDateTime(v), toDate(v) FROM system.numbers LIMIT 1, 100;

-- best case - constant stride
INSERT INTO reference (key, valueU64, valueU32, valueU16, valueU8, valueI64, valueI32, valueI16, valueI8, valueDT, valueD)
	SELECT number as n, n as v, v, v, v, v, v, v, v, toDateTime(v), toDate(v) FROM system.numbers LIMIT 101, 100;

-- checking for overflow
INSERT INTO reference (key, valueU64, valueI64)
VALUES (201, 18446744073709551616, 9223372036854775808), (202, 0, -9223372036854775808), (203, 18446744073709551616, 9223372036854775808);

-- worst case - random stride
INSERT INTO reference (key, valueU64, valueU32, valueU16, valueU8, valueI64, valueI32, valueI16, valueI8, valueDT, valueD)
	SELECT number as n, n + (rand64() - 9223372036854775808)/1000 as v, v, v, v, v, v, v, v, toDateTime(v), toDate(v) FROM system.numbers LIMIT 301, 100;


INSERT INTO doubleDelta SELECT * FROM reference;

-- same number of rows
SELECT a[1] - a[2] FROM (
	SELECT groupArray(1) AS a FROM (
		SELECT count() FROM reference
		UNION ALL
		SELECT count() FROM doubleDelta
	)
);

SELECT 'U64';
SELECT
	key,
	r.valueU64, d.valueU64, r.valueU64 - d.valueU64 as dU64
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dU64 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'U32';
SELECT
	key,
	r.valueU32, d.valueU32, r.valueU32 - d.valueU32 as dU32
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dU32 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'U16';
SELECT
	key,
	r.valueU16, d.valueU16, r.valueU16 - d.valueU16 as dU16
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dU16 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'U8';
SELECT
	key,
	r.valueU8, d.valueU8, r.valueU8 - d.valueU8 as dU8
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dU8 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'I64';
SELECT
	key,
	r.valueI64, d.valueI64, r.valueI64 - d.valueI64 as dI64
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dI64 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'I32';
SELECT
	key,
	r.valueI32, d.valueI32, r.valueI32 - d.valueI32 as dI32
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dI32 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'I16';
SELECT
	key,
	r.valueI16, d.valueI16, r.valueI16 - d.valueI16 as dI16
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dI16 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'I8';
SELECT
	key,
	r.valueI8, d.valueI8, r.valueI8 - d.valueI8 as dI8
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dI8 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'DT';
SELECT
	key,
	r.valueDT, d.valueDT, r.valueDT - d.valueDT as dDT
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dDT != 0
ORDER BY r.key
LIMIT 10;


SELECT 'D';
SELECT
	key,
	r.valueD, d.valueD, r.valueD - d.valueD as dD
FROM reference as r, doubleDelta as d
WHERE
	r.key == d.key
AND
	dD != 0
ORDER BY r.key
LIMIT 10;

-- Compatibity with other codecs
DROP TABLE IF EXISTS dd_lz4_codec;
CREATE TABLE dd_lz4_codec (
    key      UInt64   CODEC(DoubleDelta, LZ4),
    valueU64 UInt64   CODEC(DoubleDelta, LZ4),
    valueU32 UInt32   CODEC(DoubleDelta, LZ4),
    valueU16 UInt16   CODEC(DoubleDelta, LZ4),
    valueU8  UInt8    CODEC(DoubleDelta, LZ4),
    valueI64 Int64    CODEC(DoubleDelta, LZ4),
    valueI32 Int32    CODEC(DoubleDelta, LZ4),
    valueI16 Int16    CODEC(DoubleDelta, LZ4),
    valueI8  Int8     CODEC(DoubleDelta, LZ4),
    valueDT  DateTime CODEC(DoubleDelta, LZ4),
    valueD   Date     CODEC(DoubleDelta, LZ4)
) Engine = MergeTree ORDER BY key;

INSERT INTO dd_lz4_codec SELECT * FROM reference;

DROP TABLE IF EXISTS reference;
DROP TABLE IF EXISTS doubleDelta;
DROP TABLE IF EXISTS dd_lz4_codec;