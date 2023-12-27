SELECT *
FROM
(
    SELECT 1 AS a
) AS t1
INNER JOIN
(
    SELECT 1 AS a
    GROUP BY 1
        WITH TOTALS
    UNION ALL
    SELECT 1
    GROUP BY 1
        WITH TOTALS
) AS t2 USING (a);

SELECT a
FROM
(
    SELECT
        NULL AS a,
        NULL AS b,
        NULL AS c
    UNION ALL
    SELECT
        100000000000000000000.,
        NULL,
        NULL
    WHERE 0
    GROUP BY
        GROUPING SETS ((NULL))
        WITH TOTALS
) AS js1
ALL LEFT JOIN
(
    SELECT
        NULL AS a,
        2147483647 AS d
    GROUP BY
        NULL,
        '214748364.8'
        WITH CUBE
        WITH TOTALS
    UNION ALL
    SELECT
        2147483646,
        NULL
    GROUP BY
        base58Encode(materialize(NULL)),
        NULL
        WITH TOTALS
) AS js2 USING (a)
ORDER BY b ASC NULLS FIRST;
