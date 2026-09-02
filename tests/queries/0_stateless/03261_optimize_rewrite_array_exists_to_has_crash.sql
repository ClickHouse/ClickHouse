-- https://github.com/ClickHouse/ClickHouse/issues/71382
-- This query used to crash in the old analyzer's RewriteArrayExistsFunctionVisitor.
-- The old analyzer visitor has been removed; this test verifies no regression.
DROP TABLE IF EXISTS rewrite;
CREATE TABLE rewrite (c0 Int) ENGINE = Memory();
SELECT 1
FROM rewrite
INNER JOIN rewrite AS y ON (
    SELECT 1
)
INNER JOIN rewrite AS z ON 1;
DROP TABLE rewrite;
