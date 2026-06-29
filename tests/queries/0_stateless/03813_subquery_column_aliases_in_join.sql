SET enable_analyzer = 1;

SELECT * FROM (SELECT 1) AS t(a) JOIN (SELECT 1) AS u(b) ON a = b;

SELECT * FROM (SELECT 1, 2) AS t(a, b) JOIN (SELECT 1, 3) AS u(c, d) ON a = c;

SELECT * FROM (SELECT 1, 2) AS t(a, b) JOIN (SELECT 1, 3) AS u(c, d) ON a = c WHERE b = 2;

SELECT * FROM (SELECT 1) AS t(a) LEFT JOIN (SELECT 2) AS u(b) ON a = b;

SELECT a, b FROM (SELECT 1) AS t(a) JOIN (SELECT 1) AS u(b) ON a = b;

SELECT * FROM (SELECT * FROM (SELECT 1) AS inner_t(x)) AS outer_t(y) 
         JOIN (SELECT 1) AS u(z) ON y = z;

SELECT * FROM (SELECT 1 UNION ALL SELECT 2) AS t(a) JOIN (SELECT 1) AS u(b) ON a = b;

SELECT * FROM (SELECT 1, 'x' UNION ALL SELECT 2, 'y') AS t(a, b) WHERE a = 1;

SELECT a * 2 FROM ((SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3) AS t(a) ORDER BY a;

SELECT * FROM (SELECT 1, 2) AS t(a, a); -- { serverError BAD_ARGUMENTS }
