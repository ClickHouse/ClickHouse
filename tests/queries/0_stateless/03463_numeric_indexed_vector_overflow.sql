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


SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, nan), (3, 2), (0, 0), (5, 1)); -- { serverError INCORRECT_DATA }
SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, Null), (3, 2), (0, 0), (5, 1));
SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, inf), (3, 2), (0, 0), (5, 1)); -- { serverError INCORRECT_DATA }
SELECT groupNumericIndexedVector(x, y) FROM values('x Nullable(Int32), y Nullable(Float64)', (1, 0), (3, -inf), (3, 2), (0, 0), (5, 1)); -- { serverError INCORRECT_DATA }
