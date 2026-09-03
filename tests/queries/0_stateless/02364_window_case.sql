SELECT CASE
    WHEN sum(number) over () > 0 THEN number + 1
    ELSE 0 END
FROM numbers(10)
