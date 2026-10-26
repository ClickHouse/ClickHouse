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

SELECT '10'; -- quantilesDD gives the same bits as quantileDD for each level, with unsorted and repeated levels

SELECT name, arrayMap(x -> reinterpretAsUInt64(x), q) = arrayMap(x -> reinterpretAsUInt64(x), [q0, q1, q2, q3, q4, q5, q6, q7, q8, q9])
FROM
(
    SELECT name,
        quantilesDDMerge(0.01, 0.9, 0, 0.25, 0.5, 0.5, 0.75, 1, 0.01, 0.99, 0.333)(s) AS q,
        quantileDDMerge(0.01, 0.9)(s) AS q0, quantileDDMerge(0.01, 0)(s) AS q1, quantileDDMerge(0.01, 0.25)(s) AS q2,
        quantileDDMerge(0.01, 0.5)(s) AS q3, quantileDDMerge(0.01, 0.5)(s) AS q4, quantileDDMerge(0.01, 0.75)(s) AS q5,
        quantileDDMerge(0.01, 1)(s) AS q6, quantileDDMerge(0.01, 0.01)(s) AS q7, quantileDDMerge(0.01, 0.99)(s) AS q8,
        quantileDDMerge(0.01, 0.333)(s) AS q9
    FROM
    (
        SELECT 'positive' AS name, quantileDDState(0.01)(toFloat64(number + 1)) AS s FROM numbers(1000)
        UNION ALL SELECT 'negative', quantileDDState(0.01)(-toFloat64(number + 1)) FROM numbers(1000)
        UNION ALL SELECT 'zeros', quantileDDState(0.01)(toFloat64(0)) FROM numbers(10)
        UNION ALL SELECT 'mixed', quantileDDState(0.01)(toFloat64(number % 21) - 10) FROM numbers(1000)
        UNION ALL SELECT 'one value', quantileDDState(0.01)(toFloat64(-3)) FROM numbers(1)
        UNION ALL SELECT 'empty', quantileDDState(0.01)(toFloat64(number)) FROM numbers(0)
        UNION ALL SELECT 'skewed', quantileDDState(0.01)(toFloat64(if(number < 990, 1, number * 1000))) FROM numbers(1000)
        -- The bin counts 1 become 0.25: the total weight is below 1, so the ranks are negative and decrease as the level grows
        UNION ALL SELECT 'weight below 1, positive', CAST(unhex(replaceAll(hex(CAST(quantileDDState(0.01)(toFloat64(number + 1)), 'String')), '000000000000F03F', '000000000000D03F')), 'AggregateFunction(quantileDD(0.01), Float64)') FROM numbers(3)
        UNION ALL SELECT 'weight below 1, mixed', CAST(unhex(replaceAll(hex(CAST(quantileDDState(0.01)(toFloat64(number) - 1), 'String')), '000000000000F03F', '000000000000D03F')), 'AggregateFunction(quantileDD(0.01), Float64)') FROM numbers(3)
    )
    GROUP BY name
)
ORDER BY name;

SELECT quantilesDD(0.01, 0.9, 0.1, 0.5)(toInt32(number) - 500) = [quantileDD(0.01, 0.9)(toInt32(number) - 500), quantileDD(0.01, 0.1)(toInt32(number) - 500), quantileDD(0.01, 0.5)(toInt32(number) - 500)]
FROM numbers(1000);

SELECT '11'; -- The quantiles of `Date` and `DateTime` stay in the range of the type: an empty state gives 0, and the value of a bin above the largest value of the type gives that value

SELECT quantileDD(0.01)(d), quantilesDD(0.01, 0.5, 0.9)(d) FROM (SELECT toDate('2020-01-01') AS d WHERE 0);
SELECT quantileDD(0.01)(d), quantilesDD(0.01, 0.5, 0.9)(d) FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS d WHERE 0);
SELECT quantileDD(0.02)(toDate(65535)), quantilesDD(0.02, 0.5)(toDate(65535)), quantileDD(0.01)(toDate(65535));
SELECT quantileDD(0.02)(toDateTime(4294967295, 'UTC')), quantilesDD(0.02, 0.5)(toDateTime(4294967295, 'UTC'));
