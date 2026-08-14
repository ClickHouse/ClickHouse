-- Multiplying a NumericIndexedVector by an all-ones vector that has a different BSI bit
-- configuration must return the original vector, not an empty result.
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), toUInt32(2)], [toFloat64(5), toFloat64(10)])) AS vec1,
    groupNumericIndexedVectorState('BSI', 32, 16)(idx, val) AS vec2
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, vec2)) AS result
FROM (SELECT 1::UInt32 AS idx, 1.0::Float64 AS val UNION ALL SELECT 2::UInt32, 1.0::Float64)
ORDER BY result;
