SELECT
    'metric' || toString(number) as name,
    number as value,
    if(number % 2 == 0, 'info '  || toString(number), NULL) as help,
    if(number % 3 == 0, 'counter', NULL) as type
FROM numbers(5)
FORMAT Prometheus
