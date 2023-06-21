SET cross_to_inner_join_rewrite = 1;

SELECT count()
FROM numbers(4) AS n1, numbers(3) AS n2
WHERE n1.number > (select avg(n.number) from numbers(3) n);

SELECT count()
FROM numbers(4) AS n1, numbers(3) AS n2, numbers(6) AS n3
WHERE n1.number > (select avg(n.number) from numbers(3) n);
