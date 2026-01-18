-- https://github.com/ClickHouse/ClickHouse/issues/70826
SET enable_analyzer=1;
CREATE VIEW v0 AS SELECT 1 AS c0 FROM (SELECT 1) w CROSS JOIN (SELECT 1) z;
EXPLAIN SYNTAX SELECT 1 FROM v0 CROSS JOIN (SELECT 1) y CROSS JOIN (SELECT 1 x) x WHERE x.x = 1;

EXPLAIN SYNTAX WITH rhs AS (SELECT 1) SELECT lhs.d2 FROM view(SELECT dummy AS d2 FROM system.one INNER JOIN (SELECT * FROM view(SELECT dummy AS d2 FROM system.one)) AS a ON a.d2 = d2) AS lhs RIGHT JOIN rhs USING (d1);
