-- The totals row of a `WITH TOTALS` subquery used as a join right side survives a range whose output is
-- capped by the `limit` setting. Every totals-capable join algorithm reads its right side to the end,
-- totals included, before it produces a row, so the cap cannot cut the totals short. The pipeline therefore
-- does not have to read the left side to the end for such a subquery, unlike for a totals subquery that is
-- the first table expression.

SELECT l.n, r.s FROM (SELECT number AS n FROM numbers(6)) AS l INNER JOIN (SELECT number % 3 AS n, sum(number) AS s FROM numbers(6) GROUP BY n WITH TOTALS) AS r ON l.n = r.n LIMIT AFTER l.n >= 0 SETTINGS limit = 1, enable_analyzer = 0;
SELECT l.n, r.s FROM (SELECT number AS n FROM numbers(6)) AS l INNER JOIN (SELECT number % 3 AS n, sum(number) AS s FROM numbers(6) GROUP BY n WITH TOTALS) AS r ON l.n = r.n LIMIT 1 UNTIL l.n >= 5 SETTINGS limit = 1, enable_analyzer = 0;
SELECT l.n, r.s FROM (SELECT number AS n FROM numbers(6)) AS l, (SELECT number % 3 AS n, sum(number) AS s FROM numbers(6) GROUP BY n WITH TOTALS) AS r WHERE l.n = r.n LIMIT AFTER l.n >= 0 SETTINGS limit = 1, enable_analyzer = 0;

SELECT l.n, r.s FROM (SELECT number AS n FROM numbers(6)) AS l INNER JOIN (SELECT number % 3 AS n, sum(number) AS s FROM numbers(6) GROUP BY n WITH TOTALS) AS r ON l.n = r.n LIMIT AFTER l.n >= 0 SETTINGS limit = 1, enable_analyzer = 1;
SELECT l.n, r.s FROM (SELECT number AS n FROM numbers(6)) AS l INNER JOIN (SELECT number % 3 AS n, sum(number) AS s FROM numbers(6) GROUP BY n WITH TOTALS) AS r ON l.n = r.n LIMIT 1 UNTIL l.n >= 5 SETTINGS limit = 1, enable_analyzer = 1;
SELECT l.n, r.s FROM (SELECT number AS n FROM numbers(6)) AS l, (SELECT number % 3 AS n, sum(number) AS s FROM numbers(6) GROUP BY n WITH TOTALS) AS r WHERE l.n = r.n LIMIT AFTER l.n >= 0 SETTINGS limit = 1, enable_analyzer = 1;
