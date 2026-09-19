-- Vectorized sin/cos/tan promote non-Float64 inputs to Float64 before evaluation, and fall back
-- to libm for arguments above the fast-path limit (1e8) and for non-finite values. The fallback
-- must see the original argument, not the already-written approximation, so non-Float64 inputs
-- must produce the same results as the equivalent Float64 inputs.

-- Float32 above the fast-path limit.
SELECT sin(toFloat32(1e9)) = sin(toFloat64(toFloat32(1e9)));
SELECT cos(toFloat32(-1e9)) = cos(toFloat64(toFloat32(-1e9)));
SELECT tan(toFloat32(1e9)) = tan(toFloat64(toFloat32(1e9)));
-- Wrong results before the fix: sin -> 0, cos -> 1.
SELECT sin(toFloat32(1e9)) != 0 AND cos(toFloat32(-1e9)) != 1;

-- Float32 non-finite values.
SELECT isNaN(sin(toFloat32(inf))) AND isNaN(cos(toFloat32(-inf))) AND isNaN(tan(toFloat32(inf)));
SELECT isNaN(sin(toFloat32(nan))) AND isNaN(cos(toFloat32(nan))) AND isNaN(tan(toFloat32(nan)));

-- BFloat16 above the fast-path limit.
SELECT sin(toBFloat16(1e9)) = sin(toFloat64(toBFloat16(1e9)));
SELECT cos(toBFloat16(-1e9)) = cos(toFloat64(toBFloat16(-1e9)));
SELECT isNaN(sin(toBFloat16(inf))) AND isNaN(cos(toBFloat16(nan)));

-- Decimal above the fast-path limit.
SELECT sin(toDecimal64(1000000000, 2)) = sin(1000000000.0);
SELECT cos(toDecimal64(-1000000000, 2)) = cos(-1000000000.0);
SELECT tan(toDecimal128(1000000000, 2)) = tan(1000000000.0);
SELECT sin(toDecimal256(1000000000, 2)) = sin(1000000000.0);

-- Whole columns mixing in-range and out-of-range values, compared against Float64 element-wise.
SELECT sum(sin(f) = sin(toFloat64(f)) AND cos(f) = cos(toFloat64(f)) AND tan(f) = tan(toFloat64(f))) = count()
FROM (SELECT toFloat32(number * 1e8 - 5e8) AS f FROM numbers(11));
SELECT sum(sin(d) = sin(toFloat64(d)) AND cos(d) = cos(toFloat64(d))) = count()
FROM (SELECT toDecimal64(number * 100000000 - 500000000, 3) AS d FROM numbers(11));
