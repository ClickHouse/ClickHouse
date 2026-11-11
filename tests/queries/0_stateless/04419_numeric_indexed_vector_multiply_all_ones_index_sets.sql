-- Multiplying by an all-ones vector takes a fast path in BSINumericIndexedVectorData. That fast
-- path must stay equivalent to the general multiply path, which keeps the union of both index
-- sets and records an explicit zero for every index present in only one operand (multiplying by
-- a missing value yields zero). Earlier the fast path only kept the intersection, silently
-- dropping the indexes that are present in just one of the operands.

-- rhs is all-ones on indexes {2, 3, 4}; lhs has indexes {1, 2, 3}. The index 1 (lhs only) and the
-- index 4 (rhs only) must appear as explicit zeros, while 2 and 3 keep the lhs values.
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), toUInt32(2), toUInt32(3)], [toFloat64(5), toFloat64(10), toFloat64(7)])) AS vec1,
    groupNumericIndexedVectorState('BSI', 32, 16)(idx, val) AS vec2
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, vec2)) AS result
FROM (SELECT 2::UInt32 AS idx, 1.0::Float64 AS val UNION ALL SELECT 3::UInt32, 1.0::Float64 UNION ALL SELECT 4::UInt32, 1.0::Float64);

-- The same with the operands swapped, exercising the symmetric lhs-is-all-ones branch.
-- Multiplication is commutative, so the result must be identical.
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), toUInt32(2), toUInt32(3)], [toFloat64(5), toFloat64(10), toFloat64(7)])) AS vec1,
    groupNumericIndexedVectorState('BSI', 32, 16)(idx, val) AS vec2
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec2, vec1)) AS result
FROM (SELECT 2::UInt32 AS idx, 1.0::Float64 AS val UNION ALL SELECT 3::UInt32, 1.0::Float64 UNION ALL SELECT 4::UInt32, 1.0::Float64);

-- An empty (but initialized) vector has no values and is therefore not all-ones. Multiplying by it
-- must zero out the other operand through the general path, not be treated as a no-op or an
-- all-ones multiply (which would otherwise drop the result entirely).
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), toUInt32(2)], [toFloat64(5), toFloat64(10)])) AS vec1,
    groupNumericIndexedVectorState('BSI', 32, 16)(idx, val) AS vec_empty
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, vec_empty)) AS result
FROM (SELECT 1::UInt32 AS idx, 1.0::Float64 AS val WHERE 0);
