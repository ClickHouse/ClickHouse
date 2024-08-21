SET allow_experimental_analyzer = 1;

SELECT * FROM (SELECT 1 AS id) AS t1 INNER JOIN (SELECT 1 AS id) AS t2 ON t1.id = t2.id AND 1;

SELECT '--';

SELECT * FROM (SELECT 1 AS id) AS t1 INNER JOIN (SELECT 2 AS id) AS t2 ON t1.id = t2.id AND 1;

SELECT '--';

SELECT * FROM (SELECT 1 AS id) AS t1 INNER JOIN (SELECT 1 AS id) AS t2 ON t1.id = t2.id AND 0;

SELECT '--';

SELECT * FROM (SELECT 1 AS id) AS t1 INNER JOIN (SELECT 2 AS id) AS t2 ON t1.id = t2.id OR 1;

SELECT '--';

SELECT * FROM (SELECT 1 AS id, 1 AS value) AS t1 ASOF LEFT JOIN (SELECT 1 AS id, 1 AS value) AS t2 ON (t1.id = t2.id) AND 1 == 1 AND (t1.value >= t2.value);

SELECT '--';

SELECT * FROM (SELECT 1 AS id, 1 AS value) AS t1 ASOF LEFT JOIN (SELECT 1 AS id, 1 AS value) AS t2 ON (t1.id = t2.id) AND 1 != 1 AND (t1.value >= t2.value);

SELECT '--';

SELECT b.dt FROM (SELECT NULL > NULL AS pk, 1 AS dt FROM numbers(5)) AS a ASOF LEFT JOIN (SELECT NULL AS pk, 1 AS dt) AS b ON (a.pk = b.pk) AND 1 != 1 AND (a.dt >= b.dt); -- { serverError 403 }
