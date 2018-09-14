SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE test.decimal
(
    a Decimal32(4),
    b Decimal64(8),
    c Decimal128(8)
) ENGINE = Memory;

INSERT INTO test.decimal (a, b, c)
SELECT toDecimal32(number - 50, 4), toDecimal64(number - 50, 8) / 3, toDecimal128(number - 50, 8) / 5
FROM system.numbers LIMIT 101;

SELECT count(a), count(b), count(c) FROM test.decimal;
SELECT [min(a), max(a)], [min(b), max(b)], [min(c), max(c)] FROM test.decimal;

SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM test.decimal;
SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM test.decimal WHERE a > 0;
SELECT sum(a), sum(b), sum(c), sumWithOverflow(a), sumWithOverflow(b), sumWithOverflow(c) FROM test.decimal WHERE a < 0;
SELECT sum(a+1), sum(b+1), sum(c+1), sumWithOverflow(a+1), sumWithOverflow(b+1), sumWithOverflow(c+1) FROM test.decimal;
SELECT sum(a-1), sum(b-1), sum(c-1), sumWithOverflow(a-1), sumWithOverflow(b-1), sumWithOverflow(c-1) FROM test.decimal;

SELECT avg(a), avg(b), avg(c) FROM test.decimal;
SELECT avg(a), avg(b), avg(c) FROM test.decimal WHERE a > 0;
SELECT avg(a), avg(b), avg(c) FROM test.decimal WHERE a < 0;

SELECT (uniq(a), uniq(b), uniq(c)),
    (uniqCombined(a), uniqCombined(b), uniqCombined(c)),
    (uniqExact(a), uniqExact(b), uniqExact(c)),
    (uniqHLL12(a), uniqHLL12(b), uniqHLL12(c))
FROM (SELECT * FROM test.decimal ORDER BY a);

SELECT uniqUpTo(10)(a), uniqUpTo(10)(b), uniqUpTo(10)(c) FROM test.decimal WHERE a >= 0 AND a < 5;
SELECT uniqUpTo(10)(a), uniqUpTo(10)(b), uniqUpTo(10)(c) FROM test.decimal WHERE a >= 0 AND a < 10;

SELECT argMin(a, b), argMin(a, c), argMin(b, a), argMin(b, c), argMin(c, a), argMin(c, b) FROM test.decimal;
SELECT argMin(a, b), argMin(a, c), argMin(b, a), argMin(b, c), argMin(c, a), argMin(c, b) FROM test.decimal WHERE a > 0;
SELECT argMax(a, b), argMax(a, c), argMax(b, a), argMax(b, c), argMax(c, a), argMax(c, b) FROM test.decimal;
SELECT argMax(a, b), argMax(a, c), argMax(b, a), argMax(b, c), argMax(c, a), argMax(c, b) FROM test.decimal WHERE a < 0;

SELECT medianExact(a), medianExact(b), medianExact(c) FROM test.decimal;
SELECT quantileExact(a), quantileExact(b), quantileExact(c) FROM test.decimal WHERE a < 0;
SELECT quantileExact(0.0)(a), quantileExact(0.0)(b), quantileExact(0.0)(c) FROM test.decimal WHERE a >= 0;
SELECT quantileExact(0.2)(a), quantileExact(0.2)(b), quantileExact(0.2)(c) FROM test.decimal WHERE a >= 0;
SELECT quantileExact(0.4)(a), quantileExact(0.4)(b), quantileExact(0.4)(c) FROM test.decimal WHERE a >= 0;
SELECT quantileExact(0.6)(a), quantileExact(0.6)(b), quantileExact(0.6)(c) FROM test.decimal WHERE a >= 0;
SELECT quantileExact(0.8)(a), quantileExact(0.8)(b), quantileExact(0.8)(c) FROM test.decimal WHERE a >= 0;
SELECT quantileExact(1.0)(a), quantileExact(1.0)(b), quantileExact(1.0)(c) FROM test.decimal WHERE a >= 0;
SELECT quantilesExact(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a) FROM test.decimal;
SELECT quantilesExact(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b) FROM test.decimal;
SELECT quantilesExact(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c) FROM test.decimal;

-- TODO: sumMap
-- TODO: other quantile(s)
-- TODO: groupArray, groupArrayInsertAt, groupUniqArray
-- TODO: topK
