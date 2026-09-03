SET joined_subquery_requires_alias = 0;

SELECT * FROM (SELECT 1 AS id, 2 AS value);
SELECT * FROM (SELECT 1 AS id, 2 AS value, 3 AS A) SEMI LEFT JOIN (SELECT 1 AS id, 4 AS values, 5 AS D) USING id;
SELECT *, d.* FROM ( SELECT 1 AS id, 2 AS value ) SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT *, d.*, d.values FROM ( SELECT 1 AS id, 2 AS value ) SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
