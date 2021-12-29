SELECT parseDateTimeBestEffort(q0.date_field) AS parsed_date
FROM (SELECT 1 AS pk1) AS t1
INNER JOIN ( SELECT 1 AS pk1, toLowCardinality('15-JUL-16') AS date_field ) AS q0
ON q0.pk1 = t1.pk1;
