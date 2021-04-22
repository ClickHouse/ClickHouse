SELECT * FROM (
    SELECT * FROM (SELECT * FROM numbers(10)) AS a
    JOIN (SELECT * FROM numbers(10)) AS b ON a.number = b.number
    JOIN (SELECT * FROM numbers(10)) AS c ON b.number = c.number
)
WHERE a.number = 0