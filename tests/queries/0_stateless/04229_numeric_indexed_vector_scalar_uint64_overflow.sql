-- https://github.com/ClickHouse/ClickHouse/issues/104589
-- Follow-up to PR #102546: scalar pointwise ops that bypass `initializeFromVectorAndValue`
-- (`pointwiseEqual` / `pointwiseNotEqual` via inline conversion;
--  `pointwiseMultiply` / `pointwiseDivide` non-trivial path via `pointwiseRawBinaryOperate`)
-- must also reject UInt64 scalars above Int64::max consistently.

DROP TABLE IF EXISTS uin_value_u64_scalar;
CREATE TABLE uin_value_u64_scalar (uin UInt32, value UInt64) ENGINE = MergeTree() ORDER BY uin;
INSERT INTO uin_value_u64_scalar VALUES (1, 2), (2, 3);   -- not all 1s, so Multiply/Divide hit pointwiseRawBinaryOperate

-- pointwiseEqual / pointwiseNotEqual: own inline `static_cast<Int64>` at lines 1339-1341
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64_scalar) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64_scalar) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }

-- pointwiseMultiply / pointwiseDivide non-trivial path: silent clamp via `float64ToUInt64`
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64_scalar) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64_scalar) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, 18446744073709551615)); -- { serverError INCORRECT_DATA }

-- Boundary: Int64::max itself must still be accepted (no value matches Equal, so empty map).
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_u64_scalar) AS v
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(v, toUInt64(9223372036854775807)));

DROP TABLE uin_value_u64_scalar;
