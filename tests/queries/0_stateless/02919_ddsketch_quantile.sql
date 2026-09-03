SELECT '1'; -- simple test
SELECT round(quantileDD(0.01, 0.5)(number), 2) FROM numbers(200);
SELECT round(quantileDD(0.0001, 0.69)(number), 2) FROM numbers(500);
SELECT round(quantileDD(0.003, 0.42)(number), 2) FROM numbers(200);
SELECT round(quantileDD(0.02, 0.99)(number), 2) FROM numbers(500);

SELECT '2'; -- median is close to 0
SELECT round(quantileDD(0.01, 0.5)(number), 2)
FROM
(
    SELECT arrayJoin([toInt64(number), number - 10]) AS number
    FROM numbers(0, 10)
);
SELECT round(quantileDD(0.01, 0.5)(number - 10), 2) FROM numbers(21);

SELECT '3'; -- all values are negative
SELECT round(quantileDD(0.01, 0.99)(-number), 2) FROM numbers(1, 500);

SELECT '4'; -- min and max values of integer types (-2^63, 2^63-1)
SELECT round(quantileDD(0.01, 0.5)(number), 2)
FROM
(
    SELECT arrayJoin([toInt64(number), number - 9223372036854775808, toInt64(number + 9223372036854775798)]) AS number
    FROM numbers(0, 10)
);

SELECT '5'; -- min and max values of floating point types
SELECT round(quantileDD(0.01, 0.42)(number), 2)
FROM
(
    SELECT arrayJoin([toFloat32(number), number - 3.4028235e+38, toFloat32(number + 3.4028235e+38)]) AS number
    FROM numbers(0, 10)
);

SELECT '6'; -- denormalized floats
SELECT round(quantileDD(0.01, 0.69)(number), 2)
FROM
(
    SELECT arrayJoin([toFloat32(number), number - 1.1754944e-38, toFloat32(number + 1.1754944e-38)]) AS number
    FROM numbers(0, 10)
);

SELECT '7'; -- NaNs
SELECT round(quantileDD(0.01, 0.5)(number), 2)
FROM
(
    SELECT arrayJoin([toFloat32(number), NaN * number]) AS number
    FROM numbers(0, 10)
);

SELECT '8'; -- sparse sketch

SELECT round(quantileDD(0.01, 0.75)(number), 2)
FROM
(
    SELECT number * 1e7 AS number
    FROM numbers(20)
);

SELECT '9'; -- ser/deser

DROP TABLE IF EXISTS `02919_ddsketch_quantile`;

CREATE TABLE `02919_ddsketch_quantile`
ENGINE = Log AS
SELECT quantilesDDState(0.001, 0.9)(number) AS sketch
FROM numbers(1000);

INSERT INTO `02919_ddsketch_quantile` SELECT quantilesDDState(0.001, 0.9)(number + 1000)
FROM numbers(1000);

SELECT arrayMap(a -> round(a, 2), (quantilesDDMerge(0.001, 0.9)(sketch)))
FROM `02919_ddsketch_quantile`;
