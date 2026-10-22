-- `numericIndexedVector` tells an index that is absent apart from one whose value is zero, and
-- multiplication and division are computed on the original values rather than on the bit slices.
-- The result was rebuilt from the non-zero bits alone, so an index whose result came out as zero -
-- `1 / 2` in an integer type, or a product that wraps around - dropped out of the result instead of
-- staying there with a value of zero.

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (2, 7))) AS v
SELECT 'divide by a scalar', numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, 2));

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x UInt8', (1, 128), (2, 3))) AS v
SELECT 'multiply by a scalar wraps around', numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, 2));

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (2, 7))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 2), (2, 1))) AS v2
SELECT 'divide by a vector', numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v1, v2));

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x UInt8', (1, 128), (2, 3))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x UInt8', (1, 2), (2, 1))) AS v2
SELECT 'multiply by a vector wraps around', numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v1, v2));

-- Such an index is present with a value of zero, so it is counted, it is equal to zero, and it is
-- not greater than zero.
WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (2, 7))) AS v
SELECT 'cardinality', numericIndexedVectorCardinality(numericIndexedVectorPointwiseDivide(v, 2));

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (2, 7))) AS v
SELECT 'equal to zero', numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(numericIndexedVectorPointwiseDivide(v, 2), 0));

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (2, 7))) AS v
SELECT 'greater than zero', numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(numericIndexedVectorPointwiseDivide(v, 2), 0));

-- Dividing by an index that is missing gives zero, and that index keeps its explicit zero too.
WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (2, 7))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 2))) AS v2
SELECT 'divide by a missing index', numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v1, v2));

-- Dividing by a vector whose values are all one takes a fast path that returned the left operand
-- whole, so an index the divisor is missing kept its value there while the general path, which
-- divides by a missing value as if it were zero, gave zero.
WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 5), (2, 7))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1))) AS ones
SELECT 'divide by all ones', numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v1, ones));

WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 5), (2, 7))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (3, 1))) AS ones
SELECT 'divide by all ones, extra index', numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v1, ones));

-- One is not all-ones once a second value joins it, so this goes down the general path and must
-- agree with the two above.
WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 5), (2, 7))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1), (3, 2))) AS not_ones
SELECT 'divide by a vector that is not all ones', numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v1, not_ones));

-- Multiplying by an all-ones vector already kept those indexes; the two now agree.
WITH (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 5), (2, 7))) AS v1,
     (SELECT groupNumericIndexedVectorState(k, x) FROM VALUES('k UInt32, x Int8', (1, 1))) AS ones
SELECT 'multiply by all ones', numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v1, ones));
