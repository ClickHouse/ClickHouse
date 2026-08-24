set enable_analyzer = 1;

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

SELECT number
    ,lead(number, 1, 8472) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) lead
FROM numbers(5)
ORDER BY number
FORMAT Pretty; -- { serverError BAD_ARGUMENTS }

SELECT number
    ,lag(number, 1, 8472) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) lag
FROM numbers(5)
ORDER BY number
FORMAT Pretty; -- { serverError BAD_ARGUMENTS }

set enable_analyzer = 0;

SELECT number
    ,lead(number, 1, 8472) OVER () lead
FROM numbers(5)
ORDER BY number
FORMAT Pretty; -- { serverError NOT_IMPLEMENTED }

SELECT number
    ,lag(number, 1, 8472) OVER () lag
FROM numbers(5)
ORDER BY number
FORMAT Pretty; -- { serverError NOT_IMPLEMENTED }
