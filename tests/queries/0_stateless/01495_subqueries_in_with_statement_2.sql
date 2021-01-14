
WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5))
SELECT * FROM x WHERE a in (SELECT a FROM y)
ORDER BY a;

WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5))
SELECT * FROM x left JOIN y USING a
ORDER BY a;

WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5))
SELECT * FROM x JOIN y USING a
ORDER BY x.a;

WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5)),
z AS (SELECT toUInt64(1) b)
SELECT * FROM x JOIN y USING a WHERE a in (SELECT * FROM z);

WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5)),
z AS (SELECT * FROM x WHERE a % 2),
w AS (SELECT * FROM y WHERE a > 0)
SELECT * FROM x JOIN y USING a WHERE a in (SELECT * FROM z)
ORDER BY x.a;

WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5)),
z AS (SELECT * FROM x WHERE a % 2),
w AS (SELECT * FROM y WHERE a > 0)
SELECT max(a) FROM x JOIN y USING a WHERE a in (SELECT * FROM z)
HAVING a > (SELECT min(a) FROM w);

WITH 
x AS (SELECT number AS a FROM numbers(10)),
y AS (SELECT number AS a FROM numbers(5)),
z AS (SELECT * FROM x WHERE a % 2),
w AS (SELECT * FROM y WHERE a > 0)
SELECT a FROM x JOIN y USING a WHERE a in (SELECT * FROM z)
HAVING a <= (SELECT max(a) FROM w)
ORDER BY x.a;
