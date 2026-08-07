WITH a AS (SELECT t1.number AS n1, t2.number AS n2 FROM numbers(1) AS t1, numbers(1) AS t2), b AS (SELECT sum(n1) AS s FROM a)
SELECT * FROM b AS l, a AS r;

WITH a AS (SELECT t1.number AS n1, t2.number AS n2 FROM numbers(1) AS t1, numbers(1) AS t2), b AS (SELECT sum(n1) AS s FROM a)
SELECT * FROM b AS l, a AS r;

WITH a AS (SELECT number FROM numbers(1)), b AS (SELECT number FROM a) SELECT * FROM b as l, a as r;

WITH a AS (SELECT number FROM numbers(1)), b AS (SELECT number FROM a) SELECT * FROM a as l, b as r;
