
SELECT number
    ,lag(number, 1, 8472) OVER () lag
    ,LAG(number, 1, 8472) OVER () lagInsensitive
    ,lag(number, 1, 8472) OVER (ORDER BY number ASC) lagASC
    ,lag(number, 1, 8472) OVER (ORDER BY number DESC) lagDESC
    ,lead(number, 1, 8472) OVER () lead
    ,LEAD(number, 1, 8472) OVER () leadInsensitive
    ,lead(number, 1, 8472) OVER (ORDER BY number DESC) leadDESC
    ,lead(number, 1, 8472) OVER (ORDER BY number ASC) leadASC
FROM numbers(5)
ORDER BY number
FORMAT Pretty;
