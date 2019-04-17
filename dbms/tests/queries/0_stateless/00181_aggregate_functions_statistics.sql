DROP TABLE IF EXISTS series;

CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;

INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);

/* varSamp */

SELECT varSamp(x_value) FROM (SELECT x_value FROM series LIMIT 0);
SELECT varSamp(x_value) FROM (SELECT x_value FROM series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    varSamp(x_value) AS res1,
    (sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1) AS res2
FROM series
);

/* stddevSamp */

SELECT stddevSamp(x_value) FROM (SELECT x_value FROM series LIMIT 0);
SELECT stddevSamp(x_value) FROM (SELECT x_value FROM series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    stddevSamp(x_value) AS res1,
    sqrt((sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1)) AS res2
FROM series
);

/* varPop */

SELECT varPop(x_value) FROM (SELECT x_value FROM series LIMIT 0);
SELECT varPop(x_value) FROM (SELECT x_value FROM series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    varPop(x_value) AS res1,
    (sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / count() AS res2
FROM series
);

/* stddevPop */

SELECT stddevPop(x_value) FROM (SELECT x_value FROM series LIMIT 0);
SELECT stddevPop(x_value) FROM (SELECT x_value FROM series LIMIT 1);

SELECT round(abs(res1 - res2), 6) FROM
(
SELECT
    stddevPop(x_value) AS res1,
    sqrt((sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / count()) AS res2
FROM series
);

/* covarSamp */

SELECT covarSamp(x_value, y_value) FROM (SELECT x_value, y_value FROM series LIMIT 0);
SELECT covarSamp(x_value, y_value) FROM (SELECT x_value, y_value FROM series LIMIT 1);

SELECT round(abs(COVAR1 - COVAR2), 6)
FROM
(
    SELECT
        arrayJoin([1]) AS ID2,
        covarSamp(x_value, y_value) AS COVAR1
    FROM series
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
            FROM series
        ) ANY INNER JOIN
        (
            SELECT
                i AS ID,
                x_value AS X,
                y_value AS Y
            FROM series
        ) USING ID
    )
) USING ID2;

/* covarPop */

SELECT covarPop(x_value, y_value) FROM (SELECT x_value, y_value FROM series LIMIT 0);
SELECT covarPop(x_value, y_value) FROM (SELECT x_value, y_value FROM series LIMIT 1);

SELECT round(abs(COVAR1 - COVAR2), 6)
FROM
(
    SELECT
        arrayJoin([1]) AS ID2,
        covarPop(x_value, y_value) AS COVAR1
    FROM series
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
            FROM series
        ) ANY INNER JOIN
        (
            SELECT
                i AS ID,
                x_value AS X,
                y_value AS Y
            FROM series
        ) USING ID
    )
) USING ID2;

/* corr */

SELECT corr(x_value, y_value) FROM (SELECT x_value, y_value FROM series LIMIT 0);
SELECT corr(x_value, y_value) FROM (SELECT x_value, y_value FROM series LIMIT 1);

SELECT round(abs(corr(x_value, y_value) - covarPop(x_value, y_value) / (stddevPop(x_value) * stddevPop(y_value))), 6) FROM series;

/* quantile AND quantileExact */
SELECT '----quantile----';

SELECT quantileExactIf(number, number > 0) FROM numbers(90);

SELECT quantileExactIf(number, number > 100) FROM numbers(90);
SELECT quantileExactIf(toFloat32(number) , number > 100) FROM numbers(90);
SELECT quantileExactIf(toFloat64(number) , number > 100) FROM numbers(90);

SELECT quantileIf(number, number > 100) FROM numbers(90);
SELECT quantileIf(toFloat32(number) , number > 100) FROM numbers(90);
SELECT quantileIf(toFloat64(number) , number > 100) FROM numbers(90);

DROP TABLE series;
