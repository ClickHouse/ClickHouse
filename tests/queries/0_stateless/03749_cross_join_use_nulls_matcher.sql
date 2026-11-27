SET enable_analyzer = 1;

SELECT concat(*) x
FROM numbers(2) AS n1, numbers(3) AS n2
RIGHT JOIN numbers(4) AS n3
    ON n2.number = n3.number
ORDER BY x
SETTINGS join_use_nulls = true;
