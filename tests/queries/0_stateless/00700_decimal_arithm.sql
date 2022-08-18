DROP TABLE IF EXISTS decimal;

CREATE TABLE IF NOT EXISTS decimal
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
    j dec(4, 2),
    k NumEriC(23, 4),
    l numeric(9, 3),
    m NUMEric(18, 9),
    n FixED(12, 6),
    o fixed(8, 6)
) ENGINE = Memory;

INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) VALUES (42, 42, 42, 0.42, 0.42, 0.42, 42.42, 42.42, 42.42, 42.42, 42.42, 42.42, 42.42, 42.42, 42.42);
INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) VALUES (-42, -42, -42, -0.42, -0.42, -0.42, -42.42, -42.42, -42.42, -42.42, -42.42, -42.42, -42.42, -42.42, -42.42);

SELECT a + a, a - a, a * a, a / a, intDiv(a, a), intDivOrZero(a, a) FROM decimal WHERE a = 42;
SELECT b + b, b - b, b * b, b / b, intDiv(b, b), intDivOrZero(b, b) FROM decimal WHERE b = 42;
SELECT c + c, c - c, c * c, c / c, intDiv(c, c), intDivOrZero(c, c) FROM decimal WHERE c = 42;
SELECT e + e, e - e, e * e, e / e, intDiv(e, e), intDivOrZero(e, e) FROM decimal WHERE e > 0; -- { serverError 69 }
SELECT f + f, f - f, f * f, f / f, intDiv(f, f), intDivOrZero(f, f) FROM decimal WHERE f > 0; -- { serverError 69 }
SELECT g + g, g - g, g * g, g / g, intDiv(g, g), intDivOrZero(g, g) FROM decimal WHERE g > 0;
SELECT h + h, h - h, h * h, h / h, intDiv(h, h), intDivOrZero(h, h) FROM decimal WHERE h > 0; -- { serverError 407 }
SELECT h + h, h - h FROM decimal WHERE h > 0;
SELECT i + i, i - i, i * i, i / i, intDiv(i, i), intDivOrZero(i, i) FROM decimal WHERE i > 0;
SELECT i + i, i - i FROM decimal WHERE i > 0;
SELECT j + j, j - j, j * j, j / j, intDiv(j, j), intDivOrZero(j, j) FROM decimal WHERE j > 0;

SELECT a + 21, a - 21, a - 84, a * 21, a * -21, a / 21, a / 84, intDiv(a, 21), intDivOrZero(a, 84) FROM decimal WHERE a = 42;
SELECT b + 21, b - 21, b - 84, b * 21, b * -21, b / 21, b / 84, intDiv(b, 21), intDivOrZero(b, 84) FROM decimal WHERE b = 42;
SELECT c + 21, c - 21, c - 84, c * 21, c * -21, c / 21, c / 84, intDiv(c, 21), intDivOrZero(c, 84) FROM decimal WHERE c = 42;
SELECT e + 21, e - 21, e - 84, e * 21, e * -21, e / 21, e / 84 FROM decimal WHERE e > 0; -- { serverError 407 }
SELECT f + 21, f - 21, f - 84, f * 21, f * -21, f / 21, f / 84 FROM decimal WHERE f > 0;
SELECT g + 21, g - 21, g - 84, g * 21, g * -21, g / 21, g / 84, intDiv(g, 21), intDivOrZero(g, 84) FROM decimal WHERE g > 0;
SELECT h + 21, h - 21, h - 84, h * 21, h * -21, h / 21, h / 84, intDiv(h, 21), intDivOrZero(h, 84) FROM decimal WHERE h > 0;
SELECT i + 21, i - 21, i - 84, i * 21, i * -21, i / 21, i / 84, intDiv(i, 21), intDivOrZero(i, 84) FROM decimal WHERE i > 0;
SELECT j + 21, j - 21, j - 84, j * 21, j * -21, j / 21, j / 84, intDiv(j, 21), intDivOrZero(j, 84) FROM decimal WHERE j > 0;

SELECT 21 + a, 21 - a, 84 - a, 21 * a, -21 * a, 21 / a, 84 / a, intDiv(21, a), intDivOrZero(84, a) FROM decimal WHERE a = 42;
SELECT 21 + b, 21 - b, 84 - b, 21 * b, -21 * b, 21 / b, 84 / b, intDiv(21, b), intDivOrZero(84, b) FROM decimal WHERE b = 42;
SELECT 21 + c, 21 - c, 84 - c, 21 * c, -21 * c, 21 / c, 84 / c, intDiv(21, c), intDivOrZero(84, c) FROM decimal WHERE c = 42;
SELECT 21 + e, 21 - e, 84 - e, 21 * e, -21 * e, 21 / e, 84 / e FROM decimal WHERE e > 0; -- { serverError 407 }
SELECT 21 + f, 21 - f, 84 - f, 21 * f, -21 * f, 21 / f, 84 / f FROM decimal WHERE f > 0;
SELECT 21 + g, 21 - g, 84 - g, 21 * g, -21 * g, 21 / g, 84 / g, intDiv(21, g), intDivOrZero(84, g) FROM decimal WHERE g > 0;
SELECT 21 + h, 21 - h, 84 - h, 21 * h, -21 * h, 21 / h, 84 / h FROM decimal WHERE h > 0; -- { serverError 407 }
SELECT 21 + h, 21 - h, 84 - h, 21 * h, -21 * h FROM decimal WHERE h > 0;
SELECT 21 + i, 21 - i, 84 - i, 21 * i, -21 * i, 21 / i, 84 / i, intDiv(21, i), intDivOrZero(84, i) FROM decimal WHERE i > 0;
SELECT 21 + j, 21 - j, 84 - j, 21 * j, -21 * j, 21 / j, 84 / j, intDiv(21, j), intDivOrZero(84, j) FROM decimal WHERE j > 0;

SELECT a, -a, -b, -c, -d, -e, -f, -g, -h, -j from decimal ORDER BY a;
SELECT abs(a), abs(b), abs(c), abs(d), abs(e), abs(f), abs(g), abs(h), abs(j) from decimal ORDER BY a;

SET decimal_check_overflow = 0;

SELECT (h * h) != 0, (h / h) != 1 FROM decimal WHERE h > 0;
SELECT (i * i) != 0, (i / i) = 1 FROM decimal WHERE i > 0;

SELECT e + 1 > e, e + 10 > e, 1 + e > e, 10 + e > e FROM decimal WHERE e > 0;
SELECT f + 1 > f, f + 10 > f, 1 + f > f, 10 + f > f FROM decimal WHERE f > 0;

SELECT 1 / toDecimal32(0, 0); -- { serverError 153 }
SELECT 1 / toDecimal64(0, 1); -- { serverError 153 }
SELECT 1 / toDecimal128(0, 2); -- { serverError 153 }
SELECT 0 / toDecimal32(0, 3); -- { serverError 153 }
SELECT 0 / toDecimal64(0, 4); -- { serverError 153 }
SELECT 0 / toDecimal128(0, 5); -- { serverError 153 }

SELECT toDecimal32(0, 0) / toInt8(0); -- { serverError 153 }
SELECT toDecimal64(0, 1) / toInt32(0); -- { serverError 153 }
SELECT toDecimal128(0, 2) / toInt64(0); -- { serverError 153 }

SELECT toDecimal32(0, 4) AS x, multiIf(x = 0, NULL, intDivOrZero(1, x)), multiIf(x = 0, NULL, intDivOrZero(x, 0));
SELECT toDecimal64(0, 8) AS x, multiIf(x = 0, NULL, intDivOrZero(1, x)), multiIf(x = 0, NULL, intDivOrZero(x, 0));
SELECT toDecimal64(0, 18) AS x, multiIf(x = 0, NULL, intDivOrZero(1, x)), multiIf(x = 0, NULL, intDivOrZero(x, 0));

DROP TABLE IF EXISTS decimal;
