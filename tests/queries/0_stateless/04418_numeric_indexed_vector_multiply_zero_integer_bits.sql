-- Multiplying a NumericIndexedVector by a vector whose BSI configuration has integer_bit_num = 0
-- must not raise a logical error. The value 1 is represented by the bit at index fraction_bit_num,
-- which does not exist when integer_bit_num = 0, so such a vector can never be all-ones and must be
-- multiplied through the general path instead of the all-ones fast path (which would index
-- getDataArrayAt(fraction_bit_num) out of bounds). The empty-but-initialized state below is produced
-- by a value that scales to zero without being exactly zero.
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), toUInt32(2)], [toFloat64(5), toFloat64(10)])),
    (SELECT groupNumericIndexedVectorState('BSI', 0, 16)(idx, val)
     FROM (SELECT toUInt32(1) AS idx, toFloat64(0.000001) AS val))
));
