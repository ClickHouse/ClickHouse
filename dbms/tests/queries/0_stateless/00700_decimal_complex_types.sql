SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE test.decimal
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

INSERT INTO test.decimal (a, b, c, nest.a, nest.b, nest.c, tup)
    VALUES ([0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9], [1.1, 1.2], [2.1, 2.2], [3.1, 3.2], (9.1, 9.2, 9.3));

SELECT toTypeName(a), toTypeName(b), toTypeName(c) FROM test.decimal;
SELECT toTypeName(nest.a), toTypeName(nest.b), toTypeName(nest.c) FROM test.decimal;
SELECT toTypeName(a[1]), toTypeName(b[2]), toTypeName(c[3]) FROM test.decimal;
SELECT toTypeName(nest.a[1]), toTypeName(nest.b[1]), toTypeName(nest.c[1]) FROM test.decimal;
SELECT toTypeName(tup), toTypeName(tup.1), toTypeName(tup.2), toTypeName(tup.3) FROM test.decimal;

SELECT arrayJoin(a) FROM test.decimal;
SELECT arrayJoin(b) FROM test.decimal;
SELECT arrayJoin(c) FROM test.decimal;

SELECT tup, tup.1, tup.2, tup.3 FROM test.decimal;
SELECT a, arrayPopBack(a), arrayPopFront(a), arrayResize(a, 1), arraySlice(a, 2, 1) FROM test.decimal;
SELECT b, arrayPopBack(b), arrayPopFront(b), arrayResize(b, 1), arraySlice(b, 2, 1) FROM test.decimal;
SELECT c, arrayPopBack(c), arrayPopFront(c), arrayResize(c, 1), arraySlice(c, 2, 1) FROM test.decimal;
SELECT nest.a, arrayPopBack(nest.a), arrayPopFront(nest.a), arrayResize(nest.a, 1), arraySlice(nest.a, 2, 1) FROM test.decimal;
SELECT nest.b, arrayPopBack(nest.b), arrayPopFront(nest.b), arrayResize(nest.b, 1), arraySlice(nest.b, 2, 1) FROM test.decimal;
SELECT nest.c, arrayPopBack(nest.c), arrayPopFront(nest.c), arrayResize(nest.c, 1), arraySlice(nest.c, 2, 1) FROM test.decimal;
SELECT arrayPushBack(a, toDecimal32(0, 3)), arrayPushFront(a, toDecimal32(0, 3)) FROM test.decimal;
SELECT arrayPushBack(b, toDecimal64(0, 3)), arrayPushFront(b, toDecimal64(0, 3)) FROM test.decimal;
SELECT arrayPushBack(c, toDecimal128(0, 3)), arrayPushFront(c, toDecimal128(0, 3)) FROM test.decimal;

SELECT arrayPushBack(a, toDecimal32(0, 2)) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayPushBack(b, toDecimal64(0, 2)) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayPushBack(c, toDecimal128(0, 2)) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayPushFront(a, toDecimal32(0, 4)) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayPushFront(b, toDecimal64(0, 4)) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayPushFront(c, toDecimal128(0, 4)) AS x, toTypeName(x) FROM test.decimal;

SELECT length(a), length(b), length(c) FROM test.decimal;
SELECT length(nest.a), length(nest.b), length(nest.c) FROM test.decimal;
SELECT empty(a), empty(b), empty(c) FROM test.decimal;
SELECT empty(nest.a), empty(nest.b), empty(nest.c) FROM test.decimal;
SELECT notEmpty(a), notEmpty(b), notEmpty(c) FROM test.decimal;
SELECT notEmpty(nest.a), notEmpty(nest.b), notEmpty(nest.c) FROM test.decimal;
SELECT arrayUniq(a), arrayUniq(b), arrayUniq(c) FROM test.decimal;
SELECT arrayUniq(nest.a), arrayUniq(nest.b), arrayUniq(nest.c) FROM test.decimal;

SELECT has(a, toDecimal32(0.1, 3)), has(a, toDecimal32(1.0, 3)) FROM test.decimal;
SELECT has(b, toDecimal64(0.4, 3)), has(b, toDecimal64(1.0, 3)) FROM test.decimal;
SELECT has(c, toDecimal128(0.7, 3)), has(c, toDecimal128(1.0, 3)) FROM test.decimal;

SELECT has(a, toDecimal32(0.1, 2)) FROM test.decimal; -- { serverError 43 }
SELECT has(a, toDecimal32(0.1, 4)) FROM test.decimal; -- { serverError 43 }
SELECT has(a, toDecimal64(0.1, 3)) FROM test.decimal; -- { serverError 43 }
SELECT has(a, toDecimal128(0.1, 3)) FROM test.decimal; -- { serverError 43 }
SELECT has(b, toDecimal32(0.4, 3)) FROM test.decimal; -- { serverError 43 }
SELECT has(b, toDecimal64(0.4, 2)) FROM test.decimal; -- { serverError 43 }
SELECT has(b, toDecimal64(0.4, 4)) FROM test.decimal; -- { serverError 43 }
SELECT has(b, toDecimal128(0.4, 3)) FROM test.decimal; -- { serverError 43 }
SELECT has(c, toDecimal32(0.7, 3)) FROM test.decimal; -- { serverError 43 }
SELECT has(c, toDecimal64(0.7, 3)) FROM test.decimal; -- { serverError 43 }
SELECT has(c, toDecimal128(0.7, 2)) FROM test.decimal; -- { serverError 43 }
SELECT has(c, toDecimal128(0.7, 4)) FROM test.decimal; -- { serverError 43 }

SELECT indexOf(a, toDecimal32(0.1, 3)), indexOf(a, toDecimal32(1.0, 3)) FROM test.decimal;
SELECT indexOf(b, toDecimal64(0.5, 3)), indexOf(b, toDecimal64(1.0, 3)) FROM test.decimal;
SELECT indexOf(c, toDecimal128(0.9, 3)), indexOf(c, toDecimal128(1.0, 3)) FROM test.decimal;

SELECT indexOf(a, toDecimal32(0.1, 2)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(a, toDecimal32(0.1, 4)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(a, toDecimal64(0.1, 3)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(a, toDecimal128(0.1, 3)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(b, toDecimal32(0.4, 3)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(b, toDecimal64(0.4, 2)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(b, toDecimal64(0.4, 4)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(b, toDecimal128(0.4, 3)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(c, toDecimal32(0.7, 3)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(c, toDecimal64(0.7, 3)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(c, toDecimal128(0.7, 2)) FROM test.decimal; -- { serverError 43 }
SELECT indexOf(c, toDecimal128(0.7, 4)) FROM test.decimal; -- { serverError 43 }

SELECT arrayConcat(a, b) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(a, c) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(b, c) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(a, nest.a) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(b, nest.b) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(c, nest.c) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(a, nest.b) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(a, nest.c) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(b, nest.a) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(b, nest.c) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(c, nest.a) AS x, toTypeName(x) FROM test.decimal;
SELECT arrayConcat(c, nest.b) AS x, toTypeName(x) FROM test.decimal;

SELECT toDecimal32(12345.6789, 4) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x-0);
SELECT toDecimal32(-12345.6789, 4) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x+0);
SELECT toDecimal64(123456789.123456789, 9) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x-0);
SELECT toDecimal64(-123456789.123456789, 9) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x+0);
SELECT toDecimal128(0.123456789123456789, 18) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x-0);
SELECT toDecimal128(-0.1234567891123456789, 18) AS x, countEqual([x+1, x, x], x), countEqual([x, x-1, x], x), countEqual([x, x], x+0);

DROP TABLE IF EXISTS test.decimal;
