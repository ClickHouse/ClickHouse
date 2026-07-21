-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/58098
-- UNION ALL with duplicate column names (NULL) that resolve to different types
-- should not produce "Block structure mismatch" exception.

-- The exact query from the issue; the output order of a top-level UNION ALL is not
-- deterministic, so the result rows are checked by the ordered queries below.
SELECT NULL, NULL UNION ALL SELECT 'xxx', NULL FORMAT Null;

-- An ORDER BY after the last branch applies to that branch only, so wrap the union
-- in a subquery to get a deterministic global order.
SELECT * FROM (SELECT NULL, NULL UNION ALL SELECT 'xxx', NULL) ORDER BY 1 NULLS LAST;

-- Verify with more branches
SELECT * FROM (SELECT NULL, NULL UNION ALL SELECT 'xxx', NULL UNION ALL SELECT 'yyy', NULL) ORDER BY 1 NULLS LAST;

-- Single NULL column (was already working, verify no regression)
SELECT * FROM (SELECT NULL UNION ALL SELECT 'xxx') ORDER BY 1 NULLS LAST;

-- Three NULLs where only some get promoted
SELECT * FROM (SELECT NULL, NULL, NULL UNION ALL SELECT 'a', NULL, 'b') ORDER BY 1 NULLS LAST;

-- UNION DISTINCT variant
SELECT * FROM (SELECT NULL, NULL UNION DISTINCT SELECT 'xxx', NULL) ORDER BY 1 NULLS LAST;
