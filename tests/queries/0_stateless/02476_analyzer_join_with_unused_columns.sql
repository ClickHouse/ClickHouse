SET enable_analyzer = 1;

SELECT subquery_1.id, subquery_2.id FROM (SELECT 1 AS id, 2 AS value) AS subquery_1, (SELECT 3 AS id, 4 AS value) AS subquery_2;

SELECT '--';

SELECT subquery_1.value, subquery_2.value FROM (SELECT 1 AS id, 2 AS value) AS subquery_1, (SELECT 3 AS id, 4 AS value) AS subquery_2;

SELECT '--';

SELECT COLUMNS('id') FROM (SELECT 1 AS id, 2 AS value) AS subquery_1, (SELECT 3 AS id, 4 AS value) AS subquery_2;

SELECT '--';

SELECT COLUMNS('value') FROM (SELECT 1 AS id, 2 AS value) AS subquery_1, (SELECT 3 AS id, 4 AS value) AS subquery_2;

SELECT '--';

SELECT * FROM (SELECT 1 AS id, 2 AS value) AS subquery_1, (SELECT 3 AS id, 4 AS value) AS subquery_2;
