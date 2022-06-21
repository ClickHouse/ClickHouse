DROP TABLE IF EXISTS decimal;

CREATE TABLE decimal
(
    a Decimal32(4),
    b Decimal64(8),
    c Decimal128(8)
) ENGINE = Memory;

INSERT INTO decimal (a, b, c)
SELECT toDecimal32(number - 50, 4), toDecimal64(number - 50, 8) / 3, toDecimal128(number - 50, 8) / 5
FROM system.numbers LIMIT 101;

SELECT 'quantileExactWeighted';
SELECT medianExactWeighted(a, 1), medianExactWeighted(b, 2), medianExactWeighted(c, 3) as x, toTypeName(x) FROM decimal;
SELECT quantileExactWeighted(a, 1), quantileExactWeighted(b, 2), quantileExactWeighted(c, 3) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileExactWeighted(0.0)(a, 1), quantileExactWeighted(0.0)(b, 2), quantileExactWeighted(0.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.2)(a, 1), quantileExactWeighted(0.2)(b, 2), quantileExactWeighted(0.2)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.4)(a, 1), quantileExactWeighted(0.4)(b, 2), quantileExactWeighted(0.4)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.6)(a, 1), quantileExactWeighted(0.6)(b, 2), quantileExactWeighted(0.6)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(0.8)(a, 1), quantileExactWeighted(0.8)(b, 2), quantileExactWeighted(0.8)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeighted(1.0)(a, 1), quantileExactWeighted(1.0)(b, 2), quantileExactWeighted(1.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantilesExactWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a, 1) FROM decimal;
SELECT quantilesExactWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b, 2) FROM decimal;
SELECT quantilesExactWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c, 3) FROM decimal;


SELECT 'quantileApproximateWeighted';
SELECT medianApproximateWeighted(a, 1), medianApproximateWeighted(b, 2), medianApproximateWeighted(c, 3) as x, toTypeName(x) FROM decimal;
SELECT quantileApproximateWeighted(a, 1), quantileApproximateWeighted(b, 2), quantileApproximateWeighted(c, 3) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileApproximateWeighted(0.0)(a, 1), quantileApproximateWeighted(0.0)(b, 2), quantileApproximateWeighted(0.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileApproximateWeighted(0.2)(a, 1), quantileApproximateWeighted(0.2)(b, 2), quantileApproximateWeighted(0.2)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileApproximateWeighted(0.4)(a, 1), quantileApproximateWeighted(0.4)(b, 2), quantileApproximateWeighted(0.4)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileApproximateWeighted(0.6)(a, 1), quantileApproximateWeighted(0.6)(b, 2), quantileApproximateWeighted(0.6)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileApproximateWeighted(0.8)(a, 1), quantileApproximateWeighted(0.8)(b, 2), quantileApproximateWeighted(0.8)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileApproximateWeighted(1.0)(a, 1), quantileApproximateWeighted(1.0)(b, 2), quantileApproximateWeighted(1.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantilesApproximateWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a, 1) FROM decimal;
SELECT quantilesApproximateWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b, 2) FROM decimal;
SELECT quantilesApproximateWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c, 3) FROM decimal;

DROP TABLE IF EXISTS decimal;
