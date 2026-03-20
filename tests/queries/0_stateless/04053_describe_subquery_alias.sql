-- DESCRIBE TABLE (SELECT ...) AS alias should work
-- https://github.com/ClickHouse/ClickHouse/issues/100031

DESCRIBE TABLE ( SELECT 1 AS a ) AS source FORMAT TSV;
DESCRIBE TABLE ( SELECT 1 AS a, 'hello' AS b ) AS source FORMAT TSV;

DESCRIBE TABLE ( SELECT 1 AS a ) FORMAT TSV;

DESCRIBE SELECT 1 AS a FORMAT TSV;
