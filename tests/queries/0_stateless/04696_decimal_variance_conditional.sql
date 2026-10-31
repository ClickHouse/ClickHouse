-- `varPop` and friends over `Decimal` take a batched path that also covers the masked forms - the
-- `-If` combinator and `Nullable` arguments - for types narrower than 128 bits. Nothing else checks
-- those, and they differ from the plain form in that a flag decides which rows count, so a mask
-- applied to the wrong tile would go unnoticed.
--
-- Row counts span the conversion tile: 3000 crosses it twice and leaves a partial one, 100 stays
-- inside a single tile.

DROP TABLE IF EXISTS decimal_masked;

CREATE TABLE decimal_masked
(
    d32 Decimal32(4),
    d64 Decimal64(8),
    d128 Decimal128(8),
    n UInt64,
    nd32 Nullable(Decimal32(4)),
    nd64 Nullable(Decimal64(8)),
    nd128 Nullable(Decimal128(8))
) ENGINE = Memory;

INSERT INTO decimal_masked
SELECT
    toDecimal32((number % 17) * (number % 17), 4) / 3,
    toDecimal64((number % 17) * (number % 17), 8) / 3,
    toDecimal128((number % 17) * (number % 17), 8) / 3,
    number,
    if(number % 8 = 0, NULL, toDecimal32((number % 17) * (number % 17), 4) / 3),
    if(number % 8 = 0, NULL, toDecimal64((number % 17) * (number % 17), 8) / 3),
    if(number % 8 = 0, NULL, toDecimal128((number % 17) * (number % 17), 8) / 3)
FROM numbers(3000);

SELECT round(varPopIf(d32, n % 3 = 0), 6), round(varPopIf(d64, n % 3 = 0), 6), round(varPopIf(d128, n % 3 = 0), 6) FROM decimal_masked;
SELECT round(varSampIf(d32, n % 3 = 0), 6), round(varSampIf(d64, n % 3 = 0), 6), round(varSampIf(d128, n % 3 = 0), 6) FROM decimal_masked;
SELECT round(stddevPopIf(d32, n % 3 = 0), 6), round(stddevSampIf(d64, n % 3 = 0), 6) FROM decimal_masked;

SELECT round(varPop(nd32), 6), round(varPop(nd64), 6), round(varPop(nd128), 6) FROM decimal_masked;
SELECT round(stddevPop(nd32), 6), round(stddevSamp(nd64), 6) FROM decimal_masked;

-- Both flags at once: the null map and the condition have to be merged, not applied one instead of
-- the other. `count` pins how many rows survived.
SELECT round(varPopIf(nd64, n % 3 = 0), 6), countIf(nd64 IS NOT NULL AND n % 3 = 0) FROM decimal_masked;

-- The same values read as `Float64` take the path the non-`Decimal` types already used, so a
-- disagreement is a defect in the `Decimal` one. `abs` keeps a negative zero from printing as `-0`.
SELECT
    round(abs(varPopIf(d64, n % 3 = 0) - varPopIf(toFloat64(d64), n % 3 = 0)), 10),
    round(abs(varPop(nd64) - varPop(toFloat64(nd64))), 10),
    round(abs(varPopIf(nd64, n % 3 = 0) - varPopIf(toFloat64(nd64), n % 3 = 0)), 10)
FROM decimal_masked;

-- Below one tile, so the whole aggregation runs on the tail loop.
SELECT round(varPopIf(v, m % 3 = 0), 6), round(varPop(nv), 6)
FROM (
    SELECT
        toDecimal64((number % 17) * (number % 17), 8) / 3 AS v,
        number AS m,
        if(number % 8 = 0, NULL, toDecimal64((number % 17) * (number % 17), 8) / 3) AS nv
    FROM numbers(100)
);

-- A mask that keeps nothing, and one that keeps everything.
SELECT varPopIf(d64, n < 0), round(varPopIf(d64, n >= 0), 6) FROM decimal_masked;

DROP TABLE decimal_masked;
