DROP DATABASE IF EXISTS codec_test;
CREATE DATABASE codec_test;
USE codec_test;


DROP TABLE IF EXISTS reference;
DROP TABLE IF EXISTS gorilla;

CREATE TABLE reference (
    key      UInt64,
    valueF64 Float64,
    valueF32 Float32
) Engine = MergeTree ORDER BY key;


CREATE TABLE gorilla (
    key      UInt64,
    valueF64 Float64  CODEC(Gorilla),
    valueF32 Float32  CODEC(Gorilla)
) Engine = MergeTree ORDER BY key;

-- best case - same value
INSERT INTO reference (key, valueF64, valueF32)
	SELECT number AS n, e() AS v, v FROM system.numbers LIMIT 1, 100;

-- good case - values that grow insignificantly
INSERT INTO reference (key, valueF64, valueF32)
	SELECT number AS n, log2(n) AS v, v FROM system.numbers LIMIT 1001, 100;

-- bad case - values differ significantly
INSERT INTO reference (key, valueF64, valueF32)
	SELECT number AS n, n*sqrt(n) AS v, v FROM system.numbers LIMIT 2001, 100;

-- worst case - random values
INSERT INTO reference (key, valueF64, valueF32)
	SELECT number AS n, (rand64() - 9223372036854775808)/10000000000000 AS v, v FROM system.numbers LIMIT 3001, 100;


INSERT INTO gorilla SELECT * FROM reference;

SELECT a[1] - a[2] FROM (
	SELECT groupArray(1) AS a FROM (
		SELECT count() FROM reference
		UNION ALL
		SELECT count() FROM gorilla
	)
);

-- These floating-point values are expected to be BINARY equal, hence comparing the values are safe.

SELECT 'F64';
SELECT
	key,
	r.valueF64, g.valueF64, r.valueF64 - g.valueF64 AS dU64
FROM reference AS r, gorilla AS g
WHERE
	r.key == g.key
AND
	dU64 != 0
ORDER BY r.key
LIMIT 10;


SELECT 'F32';
SELECT
	key,
	r.valueF32, g.valueF32, r.valueF32 - g.valueF32 AS dU32
FROM reference AS r, gorilla AS g
WHERE
	r.key == g.key
AND
	dU32 != 0
ORDER BY r.key
LIMIT 10;


-- Compatibity with other codecs
DROP TABLE IF EXISTS g_lz4_codec;
CREATE TABLE g_lz4_codec (
    key      UInt64   CODEC(Gorilla, LZ4),
    valueU64 Float64   CODEC(Gorilla, LZ4),
    valueU32 Float32   CODEC(Gorilla, LZ4)
) Engine = MergeTree ORDER BY key;

INSERT INTO g_lz4_codec SELECT * FROM reference;

DROP TABLE IF EXISTS reference;
DROP TABLE IF EXISTS gorilla;
DROP TABLE IF EXISTS g_lz4_codec;