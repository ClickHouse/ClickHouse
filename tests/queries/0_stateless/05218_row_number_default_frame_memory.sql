SELECT sum(row_num)
FROM
(
    SELECT row_number() OVER () AS row_num
    FROM numbers(10000000)
)
SETTINGS max_memory_usage = '64Mi';
