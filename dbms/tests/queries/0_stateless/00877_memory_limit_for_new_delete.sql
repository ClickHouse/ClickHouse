SET max_memory_usage = 1000000000;

SELECT sum(ignore(*)) FROM (
    SELECT number, argMax(number, (number, toFixedString(toString(number), 1024)))
    FROM numbers(1000000)
    GROUP BY number
) -- { serverError 241 }
