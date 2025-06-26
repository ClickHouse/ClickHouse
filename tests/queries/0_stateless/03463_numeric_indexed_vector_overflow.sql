DROP TABLE IF EXISTS uin_value_details;

CREATE TABLE uin_value_details (uin UInt8, value Float64) ENGINE = MergeTree() ORDER BY uin;

INSERT INTO uin_value_details (uin, value) values (1, 7.3), (2, 8.3), (3, 0), (4, 0), (5, 0), (6, 100.6543782), (7, 0);

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, 9999999999));

WITH (SELECT groupNumericIndexedVectorState(uin, value) FROM uin_value_details) AS vec_1
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, -9999999999));

DROP TABLE uin_value_details;
