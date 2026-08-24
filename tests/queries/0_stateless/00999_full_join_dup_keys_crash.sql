SET join_use_nulls = 0;

SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';
SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';

SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';
SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';

SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';
SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';

SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';
SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';

SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';
SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';

SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';
SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';

SET join_use_nulls = 1;

SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';
SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';

SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';
SELECT * FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a, foo.b, bar.a, bar.b;
SELECT '-';

SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';
SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';

SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';
SELECT foo.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY foo.a;
SELECT '-';

SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';
SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.a = bar.b) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';

SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo FULL JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';
SELECT bar.a FROM (SELECT 1 AS a, 2 AS b) AS foo RIGHT JOIN (SELECT 1 AS a, 2 AS b) AS bar ON (foo.b = bar.a) AND (foo.b = bar.b) ORDER BY bar.a;
SELECT '-';
