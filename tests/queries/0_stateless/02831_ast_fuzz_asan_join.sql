SELECT
    '0',
    toTypeName(materialize(js2.s))
FROM
(
    SELECT number AS k
    FROM numbers(100)
) AS js1
FULL OUTER JOIN
(
    SELECT
        toLowCardinality(2147483647 + 256) AS k,
        '-0.0000000001',
        1024,
        toString(number + 10) AS s
    FROM numbers(1024)
) AS js2 ON js1.k = js2.k
ORDER BY
    inf DESC NULLS FIRST,
    js1.k ASC NULLS LAST,
    js2.k ASC
FORMAT `Null`
