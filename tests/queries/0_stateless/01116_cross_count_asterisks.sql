SELECT count(*)
FROM numbers(2) AS n1, numbers(3) AS n2, numbers(4) AS n3
WHERE (n1.number = n2.number) AND (n2.number = n3.number);

SELECT count(*) c FROM (
    SELECT count(*), count(*) as c
    FROM numbers(2) AS n1, numbers(3) AS n2, numbers(4) AS n3
    WHERE (n1.number = n2.number) AND (n2.number = n3.number)
        AND (SELECT count(*) FROM numbers(1)) = 1
)
WHERE (SELECT count(*) FROM numbers(2)) = 2
HAVING c IN(SELECT count(*) c FROM numbers(1));
