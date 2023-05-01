SET join_use_nulls = 1;

SELECT *, d.* FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;

SELECT id, toTypeName(id), value, toTypeName(value), d.values, toTypeName(d.values) FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT id, toTypeName(id), value, toTypeName(value), d.values, toTypeName(d.values) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
SELECT id, toTypeName(id), value, toTypeName(value), d.id, toTypeName(d.id) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
SELECT id, toTypeName(id), value, toTypeName(value), d.values, toTypeName(d.values) FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT id, toTypeName(id), value, toTypeName(value), d.id, toTypeName(d.id) , d.values, toTypeName(d.values) FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT id, toTypeName(id), value, toTypeName(value), d.values, toTypeName(d.values) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
SELECT id, toTypeName(id), value, toTypeName(value), d.id, toTypeName(d.id) , d.values, toTypeName(d.values) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
