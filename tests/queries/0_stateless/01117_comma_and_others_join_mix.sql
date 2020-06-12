SET join_use_nulls = 1;

SELECT *
FROM numbers(2) AS n1
JOIN numbers(3) AS n2 ON n1.number = n2.number, numbers(4) AS n3
ORDER BY n1.number, n2.number, n3.number;

SELECT '-';

SELECT *
FROM numbers(3) AS n1, numbers(2) AS n2
LEFT JOIN numbers(2) AS n3 ON n1.number = n3.number
ORDER BY n1.number, n2.number, n3.number;

SELECT '-';

SELECT *
FROM numbers(2) AS n1, numbers(3) AS n2
RIGHT JOIN numbers(4) AS n3 ON n2.number = n3.number
ORDER BY n1.number, n2.number, n3.number;
