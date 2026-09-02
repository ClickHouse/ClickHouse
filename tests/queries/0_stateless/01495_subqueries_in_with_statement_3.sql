DROP TABLE IF EXISTS cte1;
DROP TABLE IF EXISTS cte2;

CREATE TABLE cte1(a Int64) ENGINE=Memory;
CREATE TABLE cte2(a Int64) ENGINE=Memory;

INSERT INTO cte1 SELECT * FROM numbers(10000);
INSERT INTO cte2 SELECT * FROM numbers(5000);

WITH
x AS (SELECT * FROM cte1),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 2 = 1),
w AS (SELECT * FROM y WHERE a > 333)
SELECT max(a) 
FROM x JOIN y USING (a) 
WHERE a in (SELECT * FROM z) AND a <= (SELECT max(a) FROM w);

WITH
x AS (SELECT * FROM cte1),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT count(a) 
FROM x left JOIN y USING (a) 
WHERE a in (SELECT * FROM z) AND a <= (SELECT max(a) FROM w);

WITH
x AS (SELECT * FROM cte1),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT count(a) 
FROM x left JOIN y USING (a) 
WHERE a in (SELECT * FROM z);

WITH
x AS (SELECT a-4000 a FROM cte1 WHERE cte1.a >700),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT count(*) 
FROM x left JOIN y USING (a) 
WHERE a in (SELECT * FROM z);

WITH
x AS (SELECT a-4000 a FROM cte1 WHERE cte1.a >700),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT max(a), min(a), count(*) 
FROM x
WHERE a in (SELECT * FROM z) AND a <100;

WITH
x AS (SELECT a-4000 a FROM cte1 WHERE cte1.a >700),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT max(a), min(a), count(*) FROM x
WHERE  a <100;

WITH
x AS (SELECT a-4000 a FROM cte1 AS t WHERE cte1.a >700),
y AS (SELECT * FROM cte2),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT max(a), min(a), count(*) 
FROM y
WHERE  a <100;

WITH
x AS (SELECT a-4000 a FROM cte1 t WHERE t.a >700),
y AS (SELECT x.a a FROM x left JOIN cte1 USING (a)),
z AS (SELECT * FROM x WHERE a % 3 = 1),
w AS (SELECT * FROM y WHERE a > 333 AND a < 1000)
SELECT max(a), min(a), count(*) 
FROM y
WHERE a <100;

DROP TABLE cte1;
DROP TABLE cte2;
