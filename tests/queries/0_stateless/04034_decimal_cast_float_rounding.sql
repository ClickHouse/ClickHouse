-- Verify that casting floating-point values to Decimal rounds to nearest
-- instead of truncating toward zero.
-- See https://github.com/ClickHouse/ClickHouse/issues/57935

-- Scale 0: half values round away from zero
SELECT 'Scale 0 - positive half values:';
SELECT
    toFloat64(number * 2 + 1) / 2 AS x,
    CAST(x, 'Decimal(9, 0)') AS d
FROM numbers(4);

SELECT 'Scale 0 - negative half values:';
SELECT
    -toFloat64(number * 2 + 1) / 2 AS x,
    CAST(x, 'Decimal(9, 0)') AS d
FROM numbers(4);

SELECT 'Scale 0 - non-half rounding:';
SELECT CAST(toFloat64(2.7), 'Decimal(9, 0)') AS d1, CAST(toFloat64(-2.7), 'Decimal(9, 0)') AS d2;
SELECT CAST(toFloat64(2.3), 'Decimal(9, 0)') AS d1, CAST(toFloat64(-2.3), 'Decimal(9, 0)') AS d2;

-- Scale 2: use values that are exact in IEEE-754 (x.125, x.375, x.875 etc.)
SELECT 'Scale 2 - exact half values:';
SELECT CAST(toFloat64(1.125), 'Decimal(9, 2)') AS d1, CAST(toFloat64(-1.125), 'Decimal(9, 2)') AS d2;
SELECT CAST(toFloat64(2.375), 'Decimal(9, 2)') AS d1, CAST(toFloat64(-2.375), 'Decimal(9, 2)') AS d2;
SELECT CAST(toFloat64(0.875), 'Decimal(9, 2)') AS d1, CAST(toFloat64(-0.875), 'Decimal(9, 2)') AS d2;

-- Float32 input
SELECT 'Float32 input:';
SELECT CAST(toFloat32(0.5), 'Decimal(9, 0)') AS d1, CAST(toFloat32(-0.5), 'Decimal(9, 0)') AS d2;
