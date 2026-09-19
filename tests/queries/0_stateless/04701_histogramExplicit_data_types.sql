-- Int family (Int8, Int16, Int32, Int64, Int128, Int256)
SELECT histogramExplicit([-1, 2])(x), histogramExplicitOpenClosed([-1, 2])(x)
FROM (SELECT arrayJoin([toInt8(-2), toInt8(-1), toInt8(0), toInt8(1), toInt8(2), toInt8(3)]) AS x);

SELECT histogramExplicit([-1, 2])(x), histogramExplicitOpenClosed([-1, 2])(x)
FROM (SELECT arrayJoin([toInt16(-2), toInt16(-1), toInt16(0), toInt16(1), toInt16(2), toInt16(3)]) AS x);

SELECT histogramExplicit([-1, 2])(x), histogramExplicitOpenClosed([-1, 2])(x)
FROM (SELECT arrayJoin([toInt32(-2), toInt32(-1), toInt32(0), toInt32(1), toInt32(2), toInt32(3)]) AS x);

SELECT histogramExplicit([-1, 2])(x), histogramExplicitOpenClosed([-1, 2])(x)
FROM (SELECT arrayJoin([toInt64(-2), toInt64(-1), toInt64(0), toInt64(1), toInt64(2), toInt64(3)]) AS x);

SELECT histogramExplicit([-1, 2])(x), histogramExplicitOpenClosed([-1, 2])(x)
FROM (SELECT arrayJoin([toInt128(-2), toInt128(-1), toInt128(0), toInt128(1), toInt128(2), toInt128(3)]) AS x);

SELECT histogramExplicit([-1, 2])(x), histogramExplicitOpenClosed([-1, 2])(x)
FROM (SELECT arrayJoin([toInt256(-2), toInt256(-1), toInt256(0), toInt256(1), toInt256(2), toInt256(3)]) AS x);

-- UInt family (UInt8, UInt16, UInt32, UInt64, UInt128, UInt256)
SELECT histogramExplicit([1, 3])(x), histogramExplicitOpenClosed([1, 3])(x)
FROM (SELECT arrayJoin([toUInt8(0), toUInt8(1), toUInt8(2), toUInt8(3), toUInt8(4)]) AS x);

SELECT histogramExplicit([1, 3])(x), histogramExplicitOpenClosed([1, 3])(x)
FROM (SELECT arrayJoin([toUInt16(0), toUInt16(1), toUInt16(2), toUInt16(3), toUInt16(4)]) AS x);

SELECT histogramExplicit([1, 3])(x), histogramExplicitOpenClosed([1, 3])(x)
FROM (SELECT arrayJoin([toUInt32(0), toUInt32(1), toUInt32(2), toUInt32(3), toUInt32(4)]) AS x);

SELECT histogramExplicit([1, 3])(x), histogramExplicitOpenClosed([1, 3])(x)
FROM (SELECT arrayJoin([toUInt64(0), toUInt64(1), toUInt64(2), toUInt64(3), toUInt64(4)]) AS x);

SELECT histogramExplicit([1, 3])(x), histogramExplicitOpenClosed([1, 3])(x)
FROM (SELECT arrayJoin([toUInt128(0), toUInt128(1), toUInt128(2), toUInt128(3), toUInt128(4)]) AS x);

SELECT histogramExplicit([1, 3])(x), histogramExplicitOpenClosed([1, 3])(x)
FROM (SELECT arrayJoin([toUInt256(0), toUInt256(1), toUInt256(2), toUInt256(3), toUInt256(4)]) AS x);

-- Float family
SELECT histogramExplicit([1.5, 2.5])(x), histogramExplicitOpenClosed([1.5, 2.5])(x)
FROM (SELECT arrayJoin([toFloat32(1), toFloat32(1.5), toFloat32(2), toFloat32(2.5), toFloat32(3)]) AS x);

SELECT histogramExplicit([1.5, 2.5])(x), histogramExplicitOpenClosed([1.5, 2.5])(x)
FROM (SELECT arrayJoin([toFloat64(1), toFloat64(1.5), toFloat64(2), toFloat64(2.5), toFloat64(3)]) AS x);

-- Decimal family
SELECT histogramExplicit([1.5, 2.5])(x), histogramExplicitOpenClosed([1.5, 2.5])(x)
FROM (SELECT arrayJoin([toDecimal32(1.00, 2), toDecimal32(1.50, 2), toDecimal32(2.00, 2), toDecimal32(2.50, 2), toDecimal32(3.00, 2)]) AS x);

SELECT histogramExplicit([1.5, 2.5])(x), histogramExplicitOpenClosed([1.5, 2.5])(x)
FROM (SELECT arrayJoin([toDecimal64(1.00, 2), toDecimal64(1.50, 2), toDecimal64(2.00, 2), toDecimal64(2.50, 2), toDecimal64(3.00, 2)]) AS x);

SELECT histogramExplicit([1.5, 2.5])(x), histogramExplicitOpenClosed([1.5, 2.5])(x)
FROM (SELECT arrayJoin([toDecimal128(1.00, 2), toDecimal128(1.50, 2), toDecimal128(2.00, 2), toDecimal128(2.50, 2), toDecimal128(3.00, 2)]) AS x);

SELECT histogramExplicit([1.5, 2.5])(x), histogramExplicitOpenClosed([1.5, 2.5])(x)
FROM (SELECT arrayJoin([toDecimal256(1.00, 2), toDecimal256(1.50, 2), toDecimal256(2.00, 2), toDecimal256(2.50, 2), toDecimal256(3.00, 2)]) AS x);

select histogramExplicit([-5, 3])(toUInt64(number)) from numbers(5);

SELECT g, histogramExplicit([5])(number)
FROM (SELECT number, number % 2 AS g FROM numbers(10))
GROUP BY g
ORDER BY g;

select histogramExplicit([toDecimal64(1.5, 1)])(toDecimal64(number, 3))
from numbers(3);

select histogramExplicit([toDecimal64(1.5, 1)])(toFloat64(number))
from numbers(3);

select histogramExplicit([toDecimal64(1.5, 1)])(toInt32(number))
from numbers(3);

select arraySum(arrayMap(b -> b.3, tupleElement(histogramExplicit([1e300, -1e300])(toDecimal256(number, 70)), 1)))
from numbers(3);

select length(tupleElement(histogramExplicit([toDecimal256('100000000000000000001', 0), toDecimal256('100000000000000000002', 0)])(toDecimal256(number, 0)), 1))
from numbers(1);

-- Out-of-bounds boundary tests
SELECT 'u8_below_min_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toInt64(-1)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_fraction_below_min_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toFloat64(-0.4)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_min_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toUInt64(0)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_min_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toUInt64(0)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_max_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toUInt64(255)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_max_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toUInt64(255)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_fraction_above_max_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toFloat64(255.4)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'u8_above_max_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toUInt64(256)])(x) AS result FROM values('x UInt8', (0), (1), (254), (255)));

SELECT 'i8_below_min_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toInt64(-129)])(x) AS result FROM values('x Int8', (-128), (-127), (0), (126), (127)));

SELECT 'i8_min_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toInt64(-128)])(x) AS result FROM values('x Int8', (-128), (-127), (0), (126), (127)));

SELECT 'i8_min_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toInt64(-128)])(x) AS result FROM values('x Int8', (-128), (-127), (0), (126), (127)));

SELECT 'i8_max_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toInt64(127)])(x) AS result FROM values('x Int8', (-128), (-127), (0), (126), (127)));

SELECT 'i8_max_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toInt64(127)])(x) AS result FROM values('x Int8', (-128), (-127), (0), (126), (127)));

SELECT 'i8_above_max_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toInt64(128)])(x) AS result FROM values('x Int8', (-128), (-127), (0), (126), (127)));

SELECT 'decimal_below_range_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toDecimal128('-100000000000000000000000000000.00', 2)])(x) AS result FROM (SELECT arrayJoin([toDecimal64('-1.00', 2), toDecimal64('0.00', 2), toDecimal64('1.00', 2)]) AS x));

SELECT 'decimal_above_range_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toDecimal128('100000000000000000000000000000.00', 2)])(x) AS result FROM (SELECT arrayJoin([toDecimal64('-1.00', 2), toDecimal64('0.00', 2), toDecimal64('1.00', 2)]) AS x));

SELECT 'decimal_zero_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toDecimal128('0.00', 2)])(x) AS result FROM (SELECT arrayJoin([toDecimal64('-1.00', 2), toDecimal64('0.00', 2), toDecimal64('1.00', 2)]) AS x));

SELECT 'decimal_zero_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toDecimal128('0.00', 2)])(x) AS result FROM (SELECT arrayJoin([toDecimal64('-1.00', 2), toDecimal64('0.00', 2), toDecimal64('1.00', 2)]) AS x));

SELECT 'f32_large_positive_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toFloat64('1e100')])(x) AS result FROM (SELECT arrayJoin([toFloat32('-inf'), toFloat32(0), toFloat32('inf')]) AS x));

SELECT 'f32_large_positive_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toFloat64('1e100')])(x) AS result FROM (SELECT arrayJoin([toFloat32('-inf'), toFloat32(0), toFloat32('inf')]) AS x));

SELECT 'f32_large_negative_lcro', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toFloat64('-1e100')])(x) AS result FROM (SELECT arrayJoin([toFloat32('-inf'), toFloat32(0), toFloat32('inf')]) AS x));

SELECT 'f32_large_negative_oc', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicitOpenClosed([toFloat64('-1e100')])(x) AS result FROM (SELECT arrayJoin([toFloat32('-inf'), toFloat32(0), toFloat32('inf')]) AS x));

SELECT 'f32_explicit_positive_inf', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toFloat64('inf')])(x) AS result FROM (SELECT arrayJoin([toFloat32('-inf'), toFloat32(0), toFloat32('inf')]) AS x));

SELECT 'f32_explicit_negative_inf', length(result.buckets), arrayMap(bucket -> bucket.count, result.buckets)
FROM (SELECT histogramExplicit([toFloat64('-inf')])(x) AS result FROM (SELECT arrayJoin([toFloat32('-inf'), toFloat32(0), toFloat32('inf')]) AS x));
