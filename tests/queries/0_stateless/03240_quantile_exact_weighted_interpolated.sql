DROP TABLE IF EXISTS decimal;

CREATE TABLE decimal
(
    a Decimal32(4),
    b Decimal64(8),
    c Decimal128(8),
    w UInt64
) ENGINE = Memory;

INSERT INTO decimal (a, b, c, w)
SELECT toDecimal32(number - 50, 4), toDecimal64(number - 50, 8) / 3, toDecimal128(number - 50, 8) / 5, number
FROM system.numbers LIMIT 101;

SELECT 'quantileExactWeightedInterpolated';
SELECT medianExactWeightedInterpolated(a, 1), medianExactWeightedInterpolated(b, 2), medianExactWeightedInterpolated(c, 3) as x, toTypeName(x) FROM decimal;
SELECT quantileExactWeightedInterpolated(a, 1), quantileExactWeightedInterpolated(b, 2), quantileExactWeightedInterpolated(c, 3) as x, toTypeName(x) FROM decimal WHERE a < 0;
SELECT quantileExactWeightedInterpolated(0.0)(a, 1), quantileExactWeightedInterpolated(0.0)(b, 2), quantileExactWeightedInterpolated(0.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeightedInterpolated(0.2)(a, 1), quantileExactWeightedInterpolated(0.2)(b, 2), quantileExactWeightedInterpolated(0.2)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeightedInterpolated(0.4)(a, 1), quantileExactWeightedInterpolated(0.4)(b, 2), quantileExactWeightedInterpolated(0.4)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeightedInterpolated(0.6)(a, 1), quantileExactWeightedInterpolated(0.6)(b, 2), quantileExactWeightedInterpolated(0.6)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeightedInterpolated(0.8)(a, 1), quantileExactWeightedInterpolated(0.8)(b, 2), quantileExactWeightedInterpolated(0.8)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantileExactWeightedInterpolated(1.0)(a, 1), quantileExactWeightedInterpolated(1.0)(b, 2), quantileExactWeightedInterpolated(1.0)(c, 3) FROM decimal WHERE a >= 0;
SELECT quantilesExactWeightedInterpolated(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a, 1) FROM decimal;
SELECT quantilesExactWeightedInterpolated(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(b, 2) FROM decimal;
SELECT quantilesExactWeightedInterpolated(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(c, 3) FROM decimal;

SELECT 'quantileExactWeightedInterpolatedState';
SELECT quantilesExactWeightedInterpolatedMerge(0.2, 0.4, 0.6, 0.8)(x)
FROM
(
    SELECT quantilesExactWeightedInterpolatedState(0.2, 0.4, 0.6, 0.8)(number + 1, 1) AS x
    FROM numbers(49999)
);

SELECT 'Test with filter that returns no rows';
SELECT medianExactWeightedInterpolated(a, 1), medianExactWeightedInterpolated(b, 2),  medianExactWeightedInterpolated(c, 3) FROM decimal WHERE a > 1000;

SELECT 'Test with dynamic weights';
SELECT medianExactWeightedInterpolated(a, w), medianExactWeightedInterpolated(b, w), medianExactWeightedInterpolated(c, w) FROM decimal;

SELECT 'Test with all weights set to 0';
SELECT medianExactWeightedInterpolated(a, 0), medianExactWeightedInterpolated(b, 0), medianExactWeightedInterpolated(c, 0) FROM decimal;

DROP TABLE IF EXISTS decimal;
