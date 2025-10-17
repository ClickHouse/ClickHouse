SET enable_analyzer = 1;

SELECT id FROM (SELECT toLowCardinality(1) AS id) AS a INNER JOIN (SELECT toLowCardinality(toUInt128(materialize(0))) AS id) AS b USING (id) INNER JOIN a AS t ON b.id < t.id;
SELECT 1 FROM (SELECT toLowCardinality(0) a) a FULL JOIN a b USING (a) JOIN a c ON b.a > c.a SETTINGS join_use_nulls = true;
