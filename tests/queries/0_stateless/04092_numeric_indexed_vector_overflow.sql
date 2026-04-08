-- Regression test for UBSan: float-to-Int64 overflow in initializeFromVectorAndValue
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100881&sha=341df970a65dbe1925c658ad88c44e9fe1a3f005&name_0=PR&name_1=AST%20fuzzer%20%28arm_asan_ubsan%29

DROP TABLE IF EXISTS uin_value_details;

CREATE TABLE uin_value_details (uin UInt8, value Float64) ENGINE = MergeTree() ORDER BY uin;

INSERT INTO uin_value_details (uin, value) VALUES (1, 7.3), (2, 8.3);

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec, 1e26)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec, -1e26)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec, 1e26)); -- { serverError INCORRECT_DATA }

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec, 1e26)); -- { serverError INCORRECT_DATA }

DROP TABLE uin_value_details;
