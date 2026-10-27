-- https://github.com/ClickHouse/ClickHouse/issues/90205
SET enable_analyzer = 1;
SELECT c0 FROM ((SELECT 1) UNION ALL (SELECT 2)) tx(c0) ORDER BY c0;
SELECT '-';
SELECT c0 FROM ((SELECT 1) EXCEPT (SELECT 2)) tx(c0);
SELECT '-';
SELECT c0 FROM ((SELECT 1) INTERSECT (SELECT 1)) tx(c0);
