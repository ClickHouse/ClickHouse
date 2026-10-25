-- When the parser strips parentheses from IN (col) (single element),
-- it should still work as a column reference, not fail with
-- "Function 'in' is supported only if second argument is constant or table expression"

SET enable_analyzer = 1;

SELECT 3 IN (col3) FROM (SELECT 3 AS col3);
SELECT -99 IN (col3) FROM (SELECT 3 AS col3);
SELECT col0 IN (col3) FROM (SELECT 1 AS col0, 3 AS col3);
SELECT col0 NOT IN (col3) FROM (SELECT 1 AS col0, 3 AS col3);
SELECT NULL IN (col3) FROM (SELECT 3 AS col3);
