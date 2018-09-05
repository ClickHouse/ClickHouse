SET send_logs_level = 'none';
SET allow_experimental_decimal_type = 1;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE IF NOT EXISTS test.decimal
(
    a DECIMAL(9,0),
    b DECIMAL(18,0),
    c DECIMAL(38,0),
    d DECIMAL(9, 9),
    e DEC(18, 18),
    f dec(38, 38),
    g Decimal(9, 5),
    h decimal(18, 9),
    i deciMAL(38, 18),
    j dec(4,2)
) ENGINE = Memory;

INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (42, 42, 42, 0.42, 0.42, 0.42, 42.42, 42.42, 42.42, 42.42);
INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-42, -42, -42, -0.42, -0.42, -0.42, -42.42, -42.42, -42.42, -42.42);

SELECT a > toFloat64(0) FROM test.decimal; -- { serverError 43 }
SELECT g > toFloat32(0) FROM test.decimal; -- { serverError 43 }
SELECT a > '0.0' FROM test.decimal; -- { serverError 43 }

SELECT a, b, a = b, a < b, a > b, a != b, a <= b, a >= b FROM test.decimal ORDER BY a;
SELECT a, g, a = g, a < g, a > g, a != g, a <= g, a >= g FROM test.decimal ORDER BY a;
SELECT a > 0, b > 0, g > 0 FROM test.decimal ORDER BY a DESC;
SELECT a, g > toInt8(0), g > toInt16(0), g > toInt32(0), g > toInt64(0) FROM test.decimal ORDER BY a;
SELECT a, g > toUInt8(0), g > toUInt16(0), g > toUInt32(0), g > toUInt64(0) FROM test.decimal ORDER BY a;
SELECT a, b, g FROM test.decimal WHERE a IN(42) AND b IN(42) AND g IN(42);
SELECT a, b, g FROM test.decimal WHERE a > 0 AND a <= 42 AND b <= 42 AND g <= 42;

SELECT d, e, f from test.decimal WHERE d > 0 AND d < 1 AND e > 0 AND e < 1 AND f > 0 AND f < 1;
SELECT j, h, i, j from test.decimal WHERE j > 42 AND h > 42 AND h > 42 AND j > 42;
SELECT j, h, i, j from test.decimal WHERE j < 42 AND h < 42 AND h < 42 AND j < 42;
SELECT a, b, c FROM test.decimal WHERE a = toInt8(42) AND b = toInt8(42) AND c = toInt8(42);
SELECT a, b, c FROM test.decimal WHERE a = toInt16(42) AND b = toInt16(42) AND c = toInt16(42);
SELECT a, b, c FROM test.decimal WHERE a = toInt32(42) AND b = toInt32(42) AND c = toInt32(42);
SELECT a, b, c FROM test.decimal WHERE a = toInt64(42) AND b = toInt64(42) AND c = toInt64(42);
SELECT a, b, c FROM test.decimal WHERE a = toFloat32(42); -- { serverError 43 }
SELECT a, b, c FROM test.decimal WHERE a = toFloat64(42); -- { serverError 43 }

SELECT least(a, b), least(a, g), greatest(a, b), greatest(a, g) FROM test.decimal ORDER BY a;
SELECT least(a, 0), least(b, 0), least(g, 0) FROM test.decimal ORDER BY a;
SELECT greatest(a, 0), greatest(b, 0), greatest(g, 0) FROM test.decimal ORDER BY a;

SELECT (a, d, g) = (b, e, h), (a, d, g) != (b, e, h) FROM test.decimal ORDER BY a;
SELECT (a, d, g) = (c, f, i), (a, d, g) != (c, f, i) FROM test.decimal ORDER BY a;

SELECT toUInt32(2147483648) AS x, a == x FROM test.decimal WHERE a = 42; -- { serverError 407 }
SELECT toUInt64(2147483648) AS x, b == x, x == ((b - 42) + x) FROM test.decimal WHERE a = 42;
SELECT toUInt64(9223372036854775808) AS x, b == x FROM test.decimal WHERE a = 42; -- { serverError 407 }
SELECT toUInt64(9223372036854775808) AS x, c == x, x == ((c - 42) + x) FROM test.decimal WHERE a = 42;

SELECT g = 10000, (g - g + 10000) == 10000 FROM test.decimal WHERE a = 42;
SELECT 10000 = g, 10000 = (g - g + 10000) FROM test.decimal WHERE a = 42;
SELECT g = 30000 FROM test.decimal WHERE a = 42; -- { serverError 407 }
SELECT 30000 = g FROM test.decimal WHERE a = 42; -- { serverError 407 }
SELECT h = 30000, (h - g + 30000) = 30000 FROM test.decimal WHERE a = 42;
SELECT 30000 = h, 30000 = (h - g + 30000) FROM test.decimal WHERE a = 42;
SELECT h = 10000000000 FROM test.decimal WHERE a = 42; -- { serverError 407 }
SELECT i = 10000000000, (i - g + 10000000000) = 10000000000 FROM test.decimal WHERE a = 42;
SELECT 10000000000 = i, 10000000000 = (i - g + 10000000000) FROM test.decimal WHERE a = 42;

SELECT min(a), min(b), min(c), min(d), min(e), min(f), min(g), min(h), min(i), min(j) FROM test.decimal;
SELECT max(a), max(b), max(c), max(d), max(e), max(f), max(g), max(h), max(i), max(j) FROM test.decimal;

DROP TABLE IF EXISTS test.decimal;
