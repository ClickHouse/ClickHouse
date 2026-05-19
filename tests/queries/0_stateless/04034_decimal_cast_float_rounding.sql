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

-- Representable-edge rounding: values past the native type limit that round back into range must be accepted.
SELECT 'Edge values that round to representable limits:';
SELECT CAST(toFloat64(2147483647.4), 'Decimal(9, 0)') AS d1, CAST(toFloat64(-2147483647.4), 'Decimal(9, 0)') AS d2;

-- Negative edge toward INT32_MIN: a value rounding into the lower limit must be accepted;
-- anything past the limit must still throw. Decimal(9,0) holds -2147483648 exactly.
SELECT 'Negative edge rounding:';
SELECT CAST(toFloat64(-2147483647.9), 'Decimal(9, 0)');
SELECT CAST(toFloat64(-2147483648.5), 'Decimal(9, 0)'); -- { serverError DECIMAL_OVERFLOW }

-- True overflow must still throw.
SELECT 'True overflow still throws:';
SELECT CAST(toFloat64(2147483648.0), 'Decimal(9, 0)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(toFloat64(-2147483649.0), 'Decimal(9, 0)'); -- { serverError DECIMAL_OVERFLOW }

-- Float32 non-half rounding: verify the scalar/batch contract at values that Float32 can
-- represent cleanly (no boundary-precision concerns).
SELECT 'Float32 non-half rounding:';
SELECT CAST(toFloat32(2.75), 'Decimal(9, 0)') AS d1, CAST(toFloat32(-2.75), 'Decimal(9, 0)') AS d2;
SELECT CAST(toFloat32(2.25), 'Decimal(9, 0)') AS d1, CAST(toFloat32(-2.25), 'Decimal(9, 0)') AS d2;

-- Column path (vectorized batch conversion) must match the scalar path.
SELECT 'Column path matches scalar:';
WITH arrayJoin([0.5, 1.5, -0.5, -1.5, 2.7, -2.7]) AS x
SELECT toFloat64(x) AS f, CAST(f, 'Decimal(9, 0)') AS d;

-- Float32 -> Decimal256 with high scale: the multiplier overflows to +inf in Float32,
-- so a finite input still produces a non-finite product. Must surface as DECIMAL_OVERFLOW
-- instead of silently casting +inf to a wide integer.
SELECT 'Float32 -> Decimal256 high-scale non-finite multiplier:';
SELECT CAST(toFloat32(1), 'Decimal256(76)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(toFloat32(-1), 'Decimal256(76)'); -- { serverError DECIMAL_OVERFLOW }
-- Same in the column (batch) path.
SELECT CAST(materialize(toFloat32(1)), 'Decimal256(76)'); -- { serverError DECIMAL_OVERFLOW }
