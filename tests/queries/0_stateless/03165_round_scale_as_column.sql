-- Tests functions round(), roundBankers(), floor(), ceil() and trunc() with default 'scale' argument
SELECT toUInt8(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toUInt16(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toUInt32(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toUInt64(number) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt8(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt16(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt32(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toInt64(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;

SELECT toFloat32(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toFloat64(number - 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toFloat32((number - 10) / 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT toFloat64((number - 10) / 10) AS x, round(x), roundBankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;

-- Functions round(), roundBankers(), floor(), ceil() and trunc() accept non-const 'scale' arguments
SELECT toFloat32((number - 10) / 10) AS x, round(x, materialize(1)), roundBankers(x, materialize(1)), floor(x, materialize(1)), ceil(x, materialize(1)), trunc(x, materialize(1)) FROM system.numbers LIMIT 20;
SELECT toFloat64((number - 10) / 10) AS x, round(x, materialize(1)), roundBankers(x, materialize(1)), floor(x, materialize(1)), ceil(x, materialize(1)), trunc(x, materialize(1)) FROM system.numbers LIMIT 20;
SELECT toUInt8(number) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toUInt16(number) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toUInt32(number) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toUInt64(number) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;

SELECT toInt8(number - 10) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toInt16(number - 10) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toInt32(number - 10) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toInt64(number - 10) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toFloat32(number - 10) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;
SELECT toFloat64(number - 10) AS x, round(x, materialize(-1)), roundBankers(x, materialize(-1)), floor(x, materialize(-1)), ceil(x, materialize(-1)), trunc(x, materialize(-1)) FROM system.numbers LIMIT 20;

SELECT toUInt8(number) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toUInt16(number) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toUInt32(number) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toUInt64(number) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toInt8(number - 10) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toInt16(number - 10) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toInt32(number - 10) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toInt64(number - 10) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toFloat32(number - 10) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;
SELECT toFloat64(number - 10) AS x, round(x, materialize(-2)), roundBankers(x, materialize(-2)), floor(x, materialize(-2)), ceil(x, materialize(-2)), trunc(x, materialize(-2)) FROM system.numbers LIMIT 20;

SELECT toString('CHECKPOINT1');

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id Int32,
    scale Int16,
    u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
    i8 Int8, i16 Int16, i32 Int32, i64 Int64,
    f32 Float32, f64 Float64
) ENGINE = Memory;

INSERT INTO tab SELECT number    ,  0, number, number, number, number, number, number, number, number, number, number, FROM system.numbers LIMIT 20;
INSERT INTO tab SELECT number+20 ,  0, number+10, number+10, number+10, number+10, number-10, number-10, number-10, number-10, (toFloat32(number)-10)/10, (toFloat64(number)-10)/10, FROM system.numbers LIMIT 20;
INSERT INTO tab SELECT number+40 , -1, number, number, number, number, number, number, number, number, number, number, FROM system.numbers LIMIT 20;
INSERT INTO tab SELECT number+60 , -1, number+10, number+10, number+10, number+10, number-10, number-10, number-10, number-10, (toFloat32(number)-10)/10, (toFloat64(number)-10)/10, FROM system.numbers LIMIT 20;
INSERT INTO tab SELECT number+80 , -2, number, number, number, number, number, number, number, number, number, number, FROM system.numbers LIMIT 20;
INSERT INTO tab SELECT number+100, -2, number+10, number+10, number+10, number+10, number-10, number-10, number-10, number-10, (toFloat32(number)-10)/10, (toFloat64(number)-10)/10, FROM system.numbers LIMIT 20;

INSERT INTO tab SELECT number+200, -number, 0, 0, 0, 0, 0, 0, 0, 0, 12345.6789, 12345.6789, FROM system.numbers LIMIT 10;
INSERT INTO tab SELECT number+210, -number, 0, 0, 0, 0, 0, 0, 0, 0, 12345.6789, 12345.6789, FROM system.numbers LIMIT 10;

INSERT INTO tab VALUES (300, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0);
INSERT INTO tab VALUES (301, 4, 20, 20, 20, 20, 20, 20, 20, 20, 20.0, 20.0);
INSERT INTO tab VALUES (302, 4, 200, 200, 200, 200, 200, 200, 200, 200, 200.0, 200.0);
INSERT INTO tab VALUES (303, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0);
INSERT INTO tab VALUES (304, 4, 50, 50, 50, 50, 50, 50, 50, 50, 50.0, 50.0);
INSERT INTO tab VALUES (305, 4, 500, 500, 500, 500, 500, 500, 500, 500, 500.0, 500.0);

SELECT toString('id u8 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, u8   AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id u16 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, u16  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id u32 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, u32  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id u64 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, u64  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id i8 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, i8   AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id i16 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, i16  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id i32 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, i32  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id i64 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, i64  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id f32 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, f32  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;
SELECT toString('id f64 scale round(u8, scale) roundBankers(x, scale) floor(x, scale) ceil(x, scale) trunc(x, scale)');
SELECT id, f64  AS x, scale, round(x, scale), roundBankers(x, scale), floor(x, scale), ceil(x, scale), trunc(x, scale) FROM tab ORDER BY id;

DROP TABLE tab;
--
SELECT toString('CHECKPOINT2');

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id Int32,
    scale Int16,
    d32 Decimal32(4), d64 Decimal64(4), d128 Decimal128(4), d256 Decimal256(4)
) ENGINE = Memory;

INSERT INTO tab VALUES (1, 6, toDecimal32('42.42', 4), toDecimal64('42.42', 4), toDecimal128('42.42', 4), toDecimal256('42.42', 4));
INSERT INTO tab SELECT 2 , 6, cos(d32), cos(d64), cos(d128), cos(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 3 , 6, sqrt(d32), sqrt(d64), sqrt(d128), sqrt(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 4 , 6, lgamma(d32), lgamma(d64), lgamma(d128), lgamma(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 5 , 6, tgamma(d32)/1e50, tgamma(d64)/1e50, tgamma(d128)/1e50, tgamma(d256)/1e50 FROM tab WHERE id = 1;
INSERT INTO tab SELECT 6 , 8, sin(d32), sin(d64), sin(d128), sin(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 7 , 8, cos(d32), cos(d64), cos(d128), cos(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 8 , 8, log(d32), log(d64), log(d128), log(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 9 , 8, log2(d32), log2(d64), log2(d128), log2(d256) FROM tab WHERE id = 1;
INSERT INTO tab SELECT 10, 8, log10(d32), log10(d64), log10(d128), log10(d256) FROM tab WHERE id = 1;

SELECT id, round(d32, scale), round(d64, scale), round(d128, scale), round(d256, scale) FROM tab ORDER BY id;

DROP TABLE tab;

SELECT round(1, 1);
SELECT round(materialize(1), materialize(1));
SELECT round(pi(), number) FROM numbers(10);
SELECT round(toDecimal32(42.42, 2), number) from numbers(3);
SELECT round(materialize(1), 1);
SELECT materialize(10.1) AS x, ceil(x, toUInt256(123)); --{serverError ILLEGAL_TYPE_OF_ARGUMENT}
