SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

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
    g Decimal(9, 3),
    h decimal(18, 9),
    i deciMAL(38, 18),
    j dec(4,2)
) ENGINE = Memory;

INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (42, 42, 42, 0.42, 0.42, 0.42, 42.42, 42.42, 42.42, 42.42);
INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-42, -42, -42, -0.42, -0.42, -0.42, -42.42, -42.42, -42.42, -42.42);

SELECT a + a, a - a, a * a, a / a FROM test.decimal WHERE a = 42;
SELECT b + b, b - b, b * b, b / b FROM test.decimal WHERE b = 42;
SELECT c + c, c - c, c * c, c / c FROM test.decimal WHERE c = 42;
SELECT e + e, e - e, e * e, e / e FROM test.decimal WHERE e > 0; -- { serverError 69 }
SELECT f + f, f - f, f * f, f / f FROM test.decimal WHERE f > 0; -- { serverError 69 }
SELECT g + g, g - g, g * g, g / g FROM test.decimal WHERE g > 0;
SELECT h + h, h - h, h * h, h / h FROM test.decimal WHERE h > 0; -- { serverError 407 }
SELECT i + i, i - i, i * i, i / i FROM test.decimal WHERE i > 0; -- { serverError 407 }
SELECT j + j, j - j, j * j, j / j FROM test.decimal WHERE j > 0;

SELECT a + 21, a - 21, a - 84, a * 21, a * -21, a / 21, a / 84 FROM test.decimal WHERE a = 42;
SELECT b + 21, b - 21, b - 84, b * 21, b * -21, b / 21, b / 84 FROM test.decimal WHERE b = 42;
SELECT c + 21, c - 21, c - 84, c * 21, c * -21, c / 21, c / 84 FROM test.decimal WHERE c = 42;
SELECT e + 21, e - 21, e - 84, e * 21, e * -21, e / 21, e / 84 FROM test.decimal WHERE e > 0; -- { serverError 407 }
SELECT f + 21, f - 21, f - 84, f * 21, f * -21, f / 21, f / 84 FROM test.decimal WHERE f > 0; -- { serverError 407 }
SELECT g + 21, g - 21, g - 84, g * 21, g * -21, g / 21, g / 84 FROM test.decimal WHERE g > 0;
SELECT h + 21, h - 21, h - 84, h * 21, h * -21, h / 21, h / 84 FROM test.decimal WHERE h > 0;
SELECT i + 21, i - 21, i - 84, i * 21, i * -21, i / 21, i / 84 FROM test.decimal WHERE i > 0;
SELECT j + 21, j - 21, j - 84, j * 21, j * -21, j / 21, j / 84 FROM test.decimal WHERE j > 0;

SELECT 21 + a, 21 - a, 84 - a, 21 * a, -21 * a, 21 / a, 84 / a FROM test.decimal WHERE a = 42;
SELECT 21 + b, 21 - b, 84 - b, 21 * b, -21 * b, 21 / b, 84 / b FROM test.decimal WHERE b = 42;
SELECT 21 + c, 21 - c, 84 - c, 21 * c, -21 * c, 21 / c, 84 / c FROM test.decimal WHERE c = 42;
SELECT 21 + e, 21 - e, 84 - e, 21 * e, -21 * e, 21 / e, 84 / e FROM test.decimal WHERE e > 0; -- { serverError 407 }
SELECT 21 + f, 21 - f, 84 - f, 21 * f, -21 * f, 21 / f, 84 / f FROM test.decimal WHERE f > 0; -- { serverError 407 }
SELECT 21 + g, 21 - g, 84 - g, 21 * g, -21 * g, 21 / g, 84 / g FROM test.decimal WHERE g > 0;
SELECT 21 + h, 21 - h, 84 - h, 21 * h, -21 * h FROM test.decimal WHERE h > 0; --overflow 21 / h, 84 / h
SELECT 21 + i, 21 - i, 84 - i, 21 * i, -21 * i, 21 / i, 84 / i FROM test.decimal WHERE i > 0;
SELECT 21 + j, 21 - j, 84 - j, 21 * j, -21 * j, 21 / j, 84 / j FROM test.decimal WHERE j > 0;

SELECT a, -a, -b, -c, -d, -e, -f, -g, -h, -j from test.decimal ORDER BY a;
SELECT abs(a), abs(b), abs(c), abs(d), abs(e), abs(f), abs(g), abs(h), abs(j) from test.decimal ORDER BY a;

DROP TABLE IF EXISTS test.decimal;
