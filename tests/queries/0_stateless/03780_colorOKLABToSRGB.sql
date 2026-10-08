SELECT '--- Wrong arguments';
SELECT colorOKLABToSRGB(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT colorOKLABToSRGB(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLABToSRGB((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLABToSRGB((1, 'a', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLABToSRGB((1, 2, 3), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLABToSRGB((1, 2, 3), (4, 5, 6)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLABToSRGB((1, 2, 3, 4)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT '--- Regular calls';
-- Black (L=0, a=0, b=0)
WITH colorOKLABToSRGB((0, 0, 0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- White (L=1, a=0, b=0)
WITH colorOKLABToSRGB((1, 0, 0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Red-ish (positive a, neutral b)
WITH colorOKLABToSRGB((0.628, 0.2246, 0.1258)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Green-ish (negative a, positive b)
WITH colorOKLABToSRGB((0.8664, -0.2338, 0.1795)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Blue-ish (negative a, negative b)
WITH colorOKLABToSRGB((0.452, -0.0324, -0.3118)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Dark color
WITH colorOKLABToSRGB((0.0823, -0.0065, -0.0049)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Bright green
WITH colorOKLABToSRGB((0.833, -0.2113, 0.1615)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Purple-ish (positive a, negative b)
WITH colorOKLABToSRGB((0.5701, 0.0767, -0.1781)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Mid-tone neutral gray
WITH colorOKLABToSRGB((0.5, 0, 0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));


SELECT '--- Varying gamma';
-- Same color with different gamma values
WITH colorOKLABToSRGB((0.4466, 0.0991, 0.0699), 1.0) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((0.4466, 0.0991, 0.0699), 1.8) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((0.4466, 0.0991, 0.0699), 2.2) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((0.4466, 0.0991, 0.0699), 2.4) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((0.4466, 0.0991, 0.0699), 3.0) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));


SELECT '--- Edge case colors';
-- Zero chroma (achromatic - gray)
WITH colorOKLABToSRGB((0.6, 0, 0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Negative lightness (implementation-defined behavior)
WITH colorOKLABToSRGB((-0.5, 0.1, 0.05)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Lightness > 1 (implementation-defined behavior)
WITH colorOKLABToSRGB((1.5, 0.1, 0.05)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Gamma = 0 (should use fallback)
WITH colorOKLABToSRGB((0.591, 0.0910, -0.0796), 0) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Negative gamma (implementation-defined)
WITH colorOKLABToSRGB((0.591, 0.0910, -0.0796), -1000) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Very large gamma
WITH colorOKLABToSRGB((0.591, 0.0910, -0.0796), 1000) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Extreme a and b values (out of gamut)
WITH colorOKLABToSRGB((0.5, 1.0, 1.0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Very large values
WITH colorOKLABToSRGB((1e3, 1e6, 1e6)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

-- Very small positive values
WITH colorOKLABToSRGB((1e-10, 1e-10, 1e-10)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));


SELECT '--- Fuzzing';
-- Test with materialized gamma (non-constant)
WITH colorOKLABToSRGB((1e3, 1e6, 1e6), materialize(0.)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((0.5, 0.1, -0.1), materialize(2.2)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));


SELECT '--- Round-trip conversion tests';
-- These test that converting sRGB -> OKLAB -> sRGB gives back approximately the same values
WITH colorSRGBToOKLAB((128, 64, 32)) AS lab,
     colorOKLABToSRGB(lab) AS rgb_back
SELECT tuple(round(rgb_back.1, 2), round(rgb_back.2, 2), round(rgb_back.3, 2));

WITH colorSRGBToOKLAB((255, 0, 0)) AS lab,
     colorOKLABToSRGB(lab) AS rgb_back
SELECT tuple(round(rgb_back.1, 2), round(rgb_back.2, 2), round(rgb_back.3, 2));

WITH colorSRGBToOKLAB((0, 255, 0)) AS lab,
     colorOKLABToSRGB(lab) AS rgb_back
SELECT tuple(round(rgb_back.1, 2), round(rgb_back.2, 2), round(rgb_back.3, 2));

WITH colorSRGBToOKLAB((0, 0, 255)) AS lab,
     colorOKLABToSRGB(lab) AS rgb_back
SELECT tuple(round(rgb_back.1, 2), round(rgb_back.2, 2), round(rgb_back.3, 2));

WITH colorSRGBToOKLAB((128, 128, 128)) AS lab,
     colorOKLABToSRGB(lab) AS rgb_back
SELECT tuple(round(rgb_back.1, 2), round(rgb_back.2, 2), round(rgb_back.3, 2));


SELECT '--- Type variations';
-- Test with different numeric types in tuple
WITH colorOKLABToSRGB((toFloat32(0.5), toFloat64(0.1), toInt32(0))) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((toUInt8(0), toUInt16(0), toUInt32(0))) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

WITH colorOKLABToSRGB((toInt8(1), toInt16(0), toInt32(0))) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));