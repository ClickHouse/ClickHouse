SELECT k, product(x)
FROM
(
    SELECT
        toUInt8(number % 2) AS k,
        if(number % 4 < 2, 1e200, 1e-200) AS x
    FROM numbers(8)
)
GROUP BY k
ORDER BY k
SETTINGS max_threads = 1;
