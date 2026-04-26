-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/58098
-- UNION ALL with duplicate column names (NULL) that resolve to different types
-- should not produce "Block structure mismatch" exception.

SELECT NULL, NULL UNION ALL SELECT 'xxx', NULL ORDER BY 1 NULLS LAST;

-- Verify with more branches
SELECT NULL, NULL UNION ALL SELECT 'xxx', NULL UNION ALL SELECT 'yyy', NULL ORDER BY 1 NULLS LAST;

-- Single NULL column (was already working, verify no regression)
SELECT NULL UNION ALL SELECT 'xxx' ORDER BY 1 NULLS LAST;

-- Three NULLs where only some get promoted
SELECT NULL, NULL, NULL UNION ALL SELECT 'a', NULL, 'b' ORDER BY 1 NULLS LAST;

-- UNION DISTINCT variant
SELECT NULL, NULL UNION DISTINCT SELECT 'xxx', NULL ORDER BY 1 NULLS LAST;
