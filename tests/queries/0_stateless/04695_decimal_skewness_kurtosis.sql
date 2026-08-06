DROP TABLE IF EXISTS decimal_high_moments;

CREATE TABLE decimal_high_moments
(
    d32 Decimal32(4),
    d64 Decimal64(8),
    d128 Decimal128(8)
) ENGINE = Memory;

-- Squaring makes the distribution asymmetric and long-tailed, so neither the third nor the fourth
-- moment collapses to a value that a broken accumulator could reproduce by accident.
INSERT INTO decimal_high_moments
SELECT
    toDecimal32((number % 17) * (number % 17), 4) / 3,
    toDecimal64((number % 17) * (number % 17), 8) / 3,
    toDecimal128((number % 17) * (number % 17), 8) / 3
FROM numbers(3000);

SELECT round(skewPop(d32), 6), round(skewPop(d64), 6), round(skewPop(d128), 6) FROM decimal_high_moments;
SELECT round(skewSamp(d32), 6), round(skewSamp(d64), 6), round(skewSamp(d128), 6) FROM decimal_high_moments;
SELECT round(kurtPop(d32), 6), round(kurtPop(d64), 6), round(kurtPop(d128), 6) FROM decimal_high_moments;
SELECT round(kurtSamp(d32), 6), round(kurtSamp(d64), 6), round(kurtSamp(d128), 6) FROM decimal_high_moments;

-- The same values read as `Float64` go through the path that non-`Decimal` types already use, so
-- any disagreement is a defect in the `Decimal` one rather than in the shared kernel.
-- `abs` keeps a difference that rounds to a negative zero from printing as `-0`.
SELECT
    round(abs(skewPop(d64) - skewPop(toFloat64(d64))), 10),
    round(abs(skewSamp(d64) - skewSamp(toFloat64(d64))), 10),
    round(abs(kurtPop(d64) - kurtPop(toFloat64(d64))), 10),
    round(abs(kurtSamp(d64) - kurtSamp(toFloat64(d64))), 10)
FROM decimal_high_moments;

-- A row count below one tile keeps the whole aggregation on the tail loop.
SELECT round(skewPop(v), 6), round(kurtPop(v), 6)
FROM (SELECT toDecimal64((number % 17) * (number % 17), 8) / 3 AS v FROM numbers(100));

-- An exact multiple of the tile size leaves no tail at all.
SELECT round(skewPop(v), 6), round(kurtPop(v), 6)
FROM (SELECT toDecimal64((number % 17) * (number % 17), 8) / 3 AS v FROM numbers(2048));

-- `skewPop` of a sample symmetric about zero is zero regardless of how the moments are folded.
SELECT round(skewPop(toDecimal64(number, 4) - toDecimal64(1499.5, 4)), 6) FROM numbers(3000);

DROP TABLE decimal_high_moments;
