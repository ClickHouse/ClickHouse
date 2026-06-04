DROP TABLE IF EXISTS uin_value_details;

CREATE TABLE uin_value_details (uin UInt8, value Float64) ENGINE = MergeTree() ORDER BY uin;

INSERT INTO uin_value_details (uin, value) values (1, 7.3), (2, 8.3), (3, 0), (4, 0), (5, 0), (6, 100.6543782), (7, 0);

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, 9999999999));

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, -9999999999));

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, nan)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, Null));

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, inf)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, inf)); -- { serverError INCORRECT_DATA }

INSERT INTO uin_value_details (uin, value) values (1, 7.3), (2, 8.3), (3, 0), (4, 0), (5, 0), (6, 100.6543782), (7, inf);
WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, 0)); -- { serverError INCORRECT_DATA }

DROP TABLE uin_value_details;

-- https://github.com/ClickHouse/ClickHouse/issues/82239
SELECT 'Test with NaN, INFs and Nulls' AS test;

SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, nan), (3, 2), (0, 0), (5, 1)); -- { serverError INCORRECT_DATA }
SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, Null), (3, 2), (0, 0), (5, 1));
SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, inf), (3, 2), (0, 0), (5, 1)); -- { serverError INCORRECT_DATA }
SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, -inf), (3, 2), (0, 0), (5, 1)); -- { serverError INCORRECT_DATA }

-- https://github.com/ClickHouse/ClickHouse/issues/83591
SELECT 'Test for overflows' AS test;
CREATE TABLE test (t AggregateFunction(groupNumericIndexedVectorState, UInt32, Float64)) ENGINE = AggregatingMergeTree ORDER BY tuple();
CREATE TABLE test2 (t AggregateFunction(groupNumericIndexedVectorState, UInt32, UInt64)) ENGINE = AggregatingMergeTree ORDER BY tuple();
INSERT INTO test SELECT groupNumericIndexedVectorState(toUInt32(1), 1.54743e+26); -- { serverError INCORRECT_DATA }
INSERT INTO test SELECT groupNumericIndexedVectorState(toUInt32(2), -1.54743e+26); -- { serverError INCORRECT_DATA }
INSERT INTO test2 SELECT groupNumericIndexedVectorState(toUInt32(1), 18446744073709551615); -- { serverError INCORRECT_DATA }
DROP TABLE test;
DROP TABLE test2;

-- Comprehensive coverage of scalar pointwise ops routing through initializeFromVectorAndValue
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
