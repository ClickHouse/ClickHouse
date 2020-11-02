SELECT toUInt32(number * number) * number  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toUInt64(number * number) * number  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toUInt256(number * number) * number y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt32(number * number) * number   y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt64(number * number) * number   y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt128(number * number) * number  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt256(number * number) * number  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toFloat32(number * number) * number y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toFloat64(number * number) * number y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;

SELECT toUInt32(number * number) * -1  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toUInt64(number * number) * -1  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toUInt256(number * number) * -1 y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt32(number * number) * -1   y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt64(number * number) * -1   y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt128(number * number) * -1  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toInt256(number * number) * -1  y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toFloat32(number * number) * -1 y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;
SELECT toFloat64(number * number) * -1 y, toDecimal32(y, 1), toDecimal64(y, 2), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers_mt(10) ORDER BY number;

SELECT toUInt32(number * -1) * number  y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toUInt64(number * -1) * number  y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toUInt256(number * -1) * number y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toInt32(number * -1) * number   y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toInt64(number * -1) * number   y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toInt128(number * -1) * number  y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toInt256(number * -1) * number  y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toFloat32(number * -1) * number y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;
SELECT toFloat64(number * -1) * number y, toInt128(y), toInt256(y), toUInt256(y) FROM numbers_mt(10) ORDER BY number;

SELECT number y, toInt128(number) - y, toInt256(number) - y, toUInt256(number) - y FROM numbers_mt(10) ORDER BY number;
SELECT -number y, toInt128(number) + y, toInt256(number) + y, toUInt256(number) + y FROM numbers_mt(10) ORDER BY number;


SET allow_experimental_bigint_types = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64, i256 Int256, u256 UInt256, d256 Decimal256(2)) ENGINE = Memory;

INSERT INTO t SELECT number * number * number AS x, x AS i256, x AS u256, x AS d256 FROM numbers(10000);

SELECT sum(x), sum(i256), sum(u256), sum(d256) FROM t;

INSERT INTO t SELECT -number * number * number AS x, x AS i256, x AS u256, x AS d256 FROM numbers(10000);

SELECT sum(x), sum(i256), sum(u256), sum(d256) FROM t;

DROP TABLE t;
