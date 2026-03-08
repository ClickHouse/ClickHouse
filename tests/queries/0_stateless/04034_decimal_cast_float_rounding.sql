-- Verify that casting floating-point values to Decimal rounds to nearest
-- instead of truncating toward zero.
-- See https://github.com/ClickHouse/ClickHouse/issues/57935

-- Without the fix, CAST(0.5, 'Decimal(9,0)') returns 0 (truncation).
-- With the fix, it returns 1 (rounding to nearest).
SELECT 'Positive half values:';
SELECT
    toFloat64(number * 2 + 1) / 2 AS x,
    CAST(x, 'Decimal(9, 0)') AS d
FROM numbers(4);

SELECT 'Negative half values:';
SELECT
    -toFloat64(number * 2 + 1) / 2 AS x,
    CAST(x, 'Decimal(9, 0)') AS d
FROM numbers(4);

-- Non-half values should round as expected too
SELECT 'Non-half rounding:';
SELECT CAST(toFloat64(2.7), 'Decimal(9, 0)') AS d1, CAST(toFloat64(-2.7), 'Decimal(9, 0)') AS d2;
SELECT CAST(toFloat64(2.3), 'Decimal(9, 0)') AS d1, CAST(toFloat64(-2.3), 'Decimal(9, 0)') AS d2;
