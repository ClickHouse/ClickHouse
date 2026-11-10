-- Comprehensive coverage of the scalar pointwise ops that route through
-- `initializeFromVectorAndValue`. Split out of `03463_numeric_indexed_vector_overflow`, which the
-- flaky check reported as running for more than 180 seconds under MSan.

-- Float64: overflow on every scalar pointwise op that touches initializeFromVectorAndValue.
DROP TABLE IF EXISTS uin_value_f64;
CREATE TABLE uin_value_f64 (uin UInt8, value Float64) ENGINE = MergeTree() ORDER BY uin;
INSERT INTO uin_value_f64 (uin, value) VALUES (1, 7.3), (2, 8.3);

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
DROP TABLE uin_value_f64;

-- pointwiseMultiply only reaches initializeFromVectorAndValue when all values of lhs are 1.
DROP TABLE IF EXISTS uin_ones_f64;
CREATE TABLE uin_ones_f64 (uin UInt32, value Float64) ENGINE = MergeTree() ORDER BY uin;
INSERT INTO uin_ones_f64 VALUES (1, 1), (2, 1), (3, 1);
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_ones_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, 1.54743e+26)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_ones_f64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, -1.54743e+26)); -- { serverError INCORRECT_DATA }
DROP TABLE uin_ones_f64;

-- UInt64: scalar above Int64 max must be rejected through every scalar pointwise op.
DROP TABLE IF EXISTS uin_value_u64;
CREATE TABLE uin_value_u64 (uin UInt32, value UInt64) ENGINE = MergeTree() ORDER BY uin;
INSERT INTO uin_value_u64 VALUES (1, 100), (2, 200);

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
DROP TABLE uin_value_u64;

-- UInt64: pointwiseMultiply path when all values are 1.
DROP TABLE IF EXISTS uin_ones_u64;
CREATE TABLE uin_ones_u64 (uin UInt32, value UInt64) ENGINE = MergeTree() ORDER BY uin;
INSERT INTO uin_ones_u64 VALUES (1, 1), (2, 1), (3, 1);
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_ones_u64) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }
DROP TABLE uin_ones_u64;
