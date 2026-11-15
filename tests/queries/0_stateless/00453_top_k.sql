SELECT arraySort(topK(10)(n)) FROM (SELECT if(number % 100 < 10, number % 10, number) AS n FROM system.numbers LIMIT 100000);

SELECT
    k,
    arraySort(topK(v))
FROM
(
    SELECT
        number % 7 AS k,
        arrayMap(x -> arrayMap(x -> if(x = 0, NULL, toString(x)), range(x)), range(intDiv(number, 1))) AS v
    FROM system.numbers
    LIMIT 10
)
GROUP BY k
ORDER BY k ASC
