DROP TABLE IF EXISTS decimal;

CREATE TABLE decimal
(
    a Array(Decimal32(3)),
    b Array(Decimal64(3)),
    c Array(Decimal128(3)),
    nest Nested
    (
        a Decimal(9,2),
        b Decimal(18,2),
        c Decimal(38,2)
    ),
    tup Tuple(Decimal32(1), Decimal64(1), Decimal128(1))
) ENGINE = Memory;

INSERT INTO decimal (a, b, c, nest.a, nest.b, nest.c, tup)
    VALUES ([0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9], [1.1, 1.2], [2.1, 2.2], [3.1, 3.2], (9.1, 9.2, 9.3));

SELECT toTypeName(a), toTypeName(b), toTypeName(c) FROM decimal;
SELECT toTypeName(nest.a), toTypeName(nest.b), toTypeName(nest.c) FROM decimal;
SELECT toTypeName(a[1]), toTypeName(b[2]), toTypeName(c[3]) FROM decimal;
SELECT toTypeName(nest.a[1]), toTypeName(nest.b[1]), toTypeName(nest.c[1]) FROM decimal;
SELECT toTypeName(tup), toTypeName(tup.1), toTypeName(tup.2), toTypeName(tup.3) FROM decimal;

SELECT arrayJoin(a) FROM decimal;
SELECT arrayJoin(b) FROM decimal;
SELECT arrayJoin(c) FROM decimal;

SELECT tup, tup.1, tup.2, tup.3 FROM decimal;
SELECT a, arrayPopBack(a), arrayPopFront(a), arrayResize(a, 1), arraySlice(a, 2, 1) FROM decimal;
SELECT b, arrayPopBack(b), arrayPopFront(b), arrayResize(b, 1), arraySlice(b, 2, 1) FROM decimal;
SELECT c, arrayPopBack(c), arrayPopFront(c), arrayResize(c, 1), arraySlice(c, 2, 1) FROM decimal;
SELECT nest.a, arrayPopBack(nest.a), arrayPopFront(nest.a), arrayResize(nest.a, 1), arraySlice(nest.a, 2, 1) FROM decimal;
SELECT nest.b, arrayPopBack(nest.b), arrayPopFront(nest.b), arrayResize(nest.b, 1), arraySlice(nest.b, 2, 1) FROM decimal;
SELECT nest.c, arrayPopBack(nest.c), arrayPopFront(nest.c), arrayResize(nest.c, 1), arraySlice(nest.c, 2, 1) FROM decimal;
SELECT arrayPushBack(a, toDecimal32(0, 3)), arrayPushFront(a, toDecimal32(0, 3)) FROM decimal;
SELECT arrayPushBack(b, toDecimal64(0, 3)), arrayPushFront(b, toDecimal64(0, 3)) FROM decimal;
SELECT arrayPushBack(c, toDecimal128(0, 3)), arrayPushFront(c, toDecimal128(0, 3)) FROM decimal;

SELECT arrayPushBack(a, toDecimal32(0, 2)) AS x, toTypeName(x) FROM decimal;
SELECT arrayPushBack(b, toDecimal64(0, 2)) AS x, toTypeName(x) FROM decimal;
SELECT arrayPushBack(c, toDecimal128(0, 2)) AS x, toTypeName(x) FROM decimal;
SELECT arrayPushFront(a, toDecimal32(0, 4)) AS x, toTypeName(x) FROM decimal;
SELECT arrayPushFront(b, toDecimal64(0, 4)) AS x, toTypeName(x) FROM decimal;
SELECT arrayPushFront(c, toDecimal128(0, 4)) AS x, toTypeName(x) FROM decimal;

SELECT length(a), length(b), length(c) FROM decimal;
SELECT length(nest.a), length(nest.b), length(nest.c) FROM decimal;
SELECT empty(a), empty(b), empty(c) FROM decimal;
SELECT empty(nest.a), empty(nest.b), empty(nest.c) FROM decimal;
SELECT notEmpty(a), notEmpty(b), notEmpty(c) FROM decimal;
SELECT notEmpty(nest.a), notEmpty(nest.b), notEmpty(nest.c) FROM decimal;
SELECT arrayUniq(a), arrayUniq(b), arrayUniq(c) FROM decimal;
SELECT arrayUniq(nest.a), arrayUniq(nest.b), arrayUniq(nest.c) FROM decimal;

SELECT has(a, toDecimal32(0.1, 3)), has(a, toDecimal32(1.0, 3)) FROM decimal;
SELECT has(b, toDecimal64(0.4, 3)), has(b, toDecimal64(1.0, 3)) FROM decimal;
SELECT has(c, toDecimal128(0.7, 3)), has(c, toDecimal128(1.0, 3)) FROM decimal;

SELECT has(a, toDecimal32(0.1, 2)) FROM decimal;
SELECT has(a, toDecimal32(0.1, 4)) FROM decimal;
SELECT has(a, toDecimal64(0.1, 3)) FROM decimal;
SELECT has(a, toDecimal128(0.1, 3)) FROM decimal;
SELECT has(b, toDecimal32(0.4, 3)) FROM decimal;
SELECT has(b, toDecimal64(0.4, 2)) FROM decimal;
SELECT has(b, toDecimal64(0.4, 4)) FROM decimal;
SELECT has(b, toDecimal128(0.4, 3)) FROM decimal;
SELECT has(c, toDecimal32(0.7, 3)) FROM decimal;
SELECT has(c, toDecimal64(0.7, 3)) FROM decimal;
SELECT has(c, toDecimal128(0.7, 2)) FROM decimal;
SELECT has(c, toDecimal128(0.7, 4)) FROM decimal;

SELECT indexOf(a, toDecimal32(0.1, 3)), indexOf(a, toDecimal32(1.0, 3)) FROM decimal;
SELECT indexOf(b, toDecimal64(0.5, 3)), indexOf(b, toDecimal64(1.0, 3)) FROM decimal;
SELECT indexOf(c, toDecimal128(0.9, 3)), indexOf(c, toDecimal128(1.0, 3)) FROM decimal;

SELECT indexOf(a, toDecimal32(0.1, 2)) FROM decimal;
SELECT indexOf(a, toDecimal32(0.1, 4)) FROM decimal;
SELECT indexOf(a, toDecimal64(0.1, 3)) FROM decimal;
SELECT indexOf(a, toDecimal128(0.1, 3)) FROM decimal;
SELECT indexOf(b, toDecimal32(0.4, 3)) FROM decimal;
SELECT indexOf(b, toDecimal64(0.4, 2)) FROM decimal;
SELECT indexOf(b, toDecimal64(0.4, 4)) FROM decimal;
SELECT indexOf(b, toDecimal128(0.4, 3)) FROM decimal;
SELECT indexOf(c, toDecimal32(0.7, 3)) FROM decimal;
SELECT indexOf(c, toDecimal64(0.7, 3)) FROM decimal;
SELECT indexOf(c, toDecimal128(0.7, 2)) FROM decimal;
SELECT indexOf(c, toDecimal128(0.7, 4)) FROM decimal;

SELECT arrayConcat(a, b) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(a, c) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(b, c) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(a, nest.a) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(b, nest.b) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(c, nest.c) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(a, nest.b) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(a, nest.c) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(b, nest.a) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(b, nest.c) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(c, nest.a) AS x, toTypeName(x) FROM decimal;
SELECT arrayConcat(c, nest.b) AS x, toTypeName(x) FROM decimal;

SELECT toDecimal32(12345.6789, 4) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x-0);
SELECT toDecimal32(-12345.6789, 4) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x+0);
SELECT toDecimal64(123456789.123456789, 9) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x-0);
SELECT toDecimal64(-123456789.123456789, 9) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x+0);
SELECT toDecimal128(0.123456789123456789, 18) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x-0);
SELECT toDecimal128(-0.1234567891123456789, 18) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x+0);

SELECT toTypeName(x) FROM (SELECT toDecimal32('1234.5', 5) AS x UNION ALL SELECT toInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('1234.5', 5) AS x UNION ALL SELECT toUInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('12345.0', 4) AS x UNION ALL SELECT toInt16(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('12345.0', 4) AS x UNION ALL SELECT toUInt16(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal32('12.345', 7) AS x UNION ALL SELECT toInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('12.345', 7) AS x UNION ALL SELECT toUInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('1234.5', 5) AS x UNION ALL SELECT toInt16(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('1234.5', 5) AS x UNION ALL SELECT toUInt16(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal32('12345.00', 4) AS x UNION ALL SELECT toInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('12345.00', 4) AS x UNION ALL SELECT toUInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('12345.00', 4) AS x UNION ALL SELECT toInt64(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal32('12345.00', 4) AS x UNION ALL SELECT toUInt64(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toUInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toInt16(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toUInt16(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toUInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toInt64(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345.00', 4) AS x UNION ALL SELECT toUInt64(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toUInt8(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toInt16(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toUInt16(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toUInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toInt64(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT toUInt64(0) AS x) WHERE x = 0;

SELECT toTypeName(x) FROM (SELECT toDecimal32('12345', 0) AS x UNION ALL SELECT toInt32(0) AS x) WHERE x = 0;
SELECT toTypeName(x) FROM (SELECT toDecimal64('12345', 0) AS x UNION ALL SELECT toInt64(0) AS x) WHERE x = 0;

SELECT number % 2 ? toDecimal32('32.1', 5) : toDecimal32('32.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal32('32.1', 5) : toDecimal64('64.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal32('32.1', 5) : toDecimal128('128.2', 5) FROM system.numbers LIMIT 2;

SELECT number % 2 ? toDecimal64('64.1', 5) : toDecimal32('32.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal64('64.1', 5) : toDecimal64('64.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal64('64.1', 5) : toDecimal128('128.2', 5) FROM system.numbers LIMIT 2;

SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal32('32.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal64('64.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal128('128.2', 5) FROM system.numbers LIMIT 2;

SELECT number % 2 ? toDecimal32('32.1', 5) : toDecimal32('32.2', 1) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }
SELECT number % 2 ? toDecimal32('32.1', 5) : toDecimal64('64.2', 2) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }
SELECT number % 2 ? toDecimal32('32.1', 5) : toDecimal128('128.2', 3) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }

SELECT number % 2 ? toDecimal64('64.1', 5) : toDecimal32('32.2', 1) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }
SELECT number % 2 ? toDecimal64('64.1', 5) : toDecimal64('64.2', 2) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }
SELECT number % 2 ? toDecimal64('64.1', 5) : toDecimal128('128.2', 3) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }

SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal32('32.2', 1) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }
SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal64('64.2', 2) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }
SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal128('128.2', 3) FROM system.numbers LIMIT 2; -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS decimal;
