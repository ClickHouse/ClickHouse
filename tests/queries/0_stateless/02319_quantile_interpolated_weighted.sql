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

SELECT 'quantileInterpolatedWeighted';
SELECT medianInterpolatedWeighted(a, 1), medianInterpolatedWeighted(b, 2), medianInterpolatedWeighted(c, 3) as x, toTypeName(x) FROM decimal;
SELECT quantileInterpolatedWeighted(a, 1), quantileInterpolatedWeighted(b, 2), quantileInterpolatedWeighted(c, 3) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileInterpolatedWeighted(0.0)(a, 1), quantileInterpolatedWeighted(0.0)(b, 2), quantileInterpolatedWeighted(0.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileInterpolatedWeighted(0.2)(a, 1), quantileInterpolatedWeighted(0.2)(b, 2), quantileInterpolatedWeighted(0.2)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileInterpolatedWeighted(0.4)(a, 1), quantileInterpolatedWeighted(0.4)(b, 2), quantileInterpolatedWeighted(0.4)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileInterpolatedWeighted(0.6)(a, 1), quantileInterpolatedWeighted(0.6)(b, 2), quantileInterpolatedWeighted(0.6)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileInterpolatedWeighted(0.8)(a, 1), quantileInterpolatedWeighted(0.8)(b, 2), quantileInterpolatedWeighted(0.8)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileInterpolatedWeighted(1.0)(a, 1), quantileInterpolatedWeighted(1.0)(b, 2), quantileInterpolatedWeighted(1.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantilesInterpolatedWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a, 1) FROM decimal;
SELECT quantilesInterpolatedWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b, 2) FROM decimal;
SELECT quantilesInterpolatedWeighted(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c, 3) FROM decimal;

DROP TABLE IF EXISTS decimal;
