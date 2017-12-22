DROP TABLE IF EXISTS test.series;

CREATE TABLE test.series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;

INSERT INTO test.series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);

/* varSampStable */

SELECT varSampStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 0);
SELECT varSampStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    varSampStable(x_value) AS res1,
    (sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1) AS res2
FROM test.series
);

/* stddevSampStable */

SELECT stddevSampStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 0);
SELECT stddevSampStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    stddevSampStable(x_value) AS res1,
    sqrt((sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1)) AS res2
FROM test.series
);

/* varPopStable */

SELECT varPopStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 0);
SELECT varPopStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    varPopStable(x_value) AS res1,
    (sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / count() AS res2
FROM test.series
);

/* stddevPopStable */

SELECT stddevPopStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 0);
SELECT stddevPopStable(x_value) FROM (SELECT x_value FROM test.series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    stddevPopStable(x_value) AS res1,
    sqrt((sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / count()) AS res2
FROM test.series
);

/* covarSampStable */

SELECT covarSampStable(x_value, y_value) FROM (SELECT x_value, y_value FROM test.series LIMIT 0);
SELECT covarSampStable(x_value, y_value) FROM (SELECT x_value, y_value FROM test.series LIMIT 1);

SELECT round(abs(COVAR1 - COVAR2), 6)
FROM
(
    SELECT
        arrayJoin([1]) AS ID2,
        covarSampStable(x_value, y_value) AS COVAR1
    FROM test.series
) ANY INNER JOIN
(
    SELECT
        arrayJoin([1]) AS ID2,
        sum(VAL) / (count() - 1) AS COVAR2
    FROM
    (
        SELECT (X - AVG_X) * (Y - AVG_Y) AS VAL
        FROM
        (
            SELECT
                toUInt32(arrayJoin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])) AS ID,
                avg(x_value) AS AVG_X,
                avg(y_value) AS AVG_Y
            FROM test.series
        ) ANY INNER JOIN
        (
            SELECT
                i AS ID,
                x_value AS X,
                y_value AS Y
            FROM test.series
        ) USING ID
    )
) USING ID2;

/* covarPopStable */

SELECT covarPopStable(x_value, y_value) FROM (SELECT x_value, y_value FROM test.series LIMIT 0);
SELECT covarPopStable(x_value, y_value) FROM (SELECT x_value, y_value FROM test.series LIMIT 1);

SELECT round(abs(COVAR1 - COVAR2), 6)
FROM
(
    SELECT
        arrayJoin([1]) AS ID2,
        covarPopStable(x_value, y_value) AS COVAR1
    FROM test.series
) ANY INNER JOIN
(
    SELECT
        arrayJoin([1]) AS ID2,
        sum(VAL) / count() AS COVAR2
    FROM
    (
        SELECT (X - AVG_X) * (Y - AVG_Y) AS VAL
        FROM
        (
            SELECT
                toUInt32(arrayJoin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])) AS ID,
                avg(x_value) AS AVG_X,
                avg(y_value) AS AVG_Y
            FROM test.series
        ) ANY INNER JOIN
        (
            SELECT
                i AS ID,
                x_value AS X,
                y_value AS Y
            FROM test.series
        ) USING ID
    )
) USING ID2;

/* corr */

SELECT corrStable(x_value, y_value) FROM (SELECT x_value, y_value FROM test.series LIMIT 0);
SELECT corrStable(x_value, y_value) FROM (SELECT x_value, y_value FROM test.series LIMIT 1);

SELECT round(abs(corrStable(x_value, y_value) - covarPopStable(x_value, y_value) / (stddevPopStable(x_value) * stddevPopStable(y_value))), 6) FROM test.series;

DROP TABLE test.series;
