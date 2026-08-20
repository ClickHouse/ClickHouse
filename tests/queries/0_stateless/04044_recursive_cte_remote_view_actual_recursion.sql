-- Tags: no-fasttest
-- Regression test: truly-recursive CTE with remote() + view().
-- The PR test (04028) uses a non-self-referencing branch, so isStorageUsedInTree
-- only exercises the null-storage guard without finding a CTE self-reference.
-- This test exercises the path where the function must traverse past the
-- null-storage view() node and still find the CTE self-reference to enable
-- actual recursion. (Fix in src/Analyzer/Utils.cpp:107, PR #99081.)

SET enable_analyzer=1;

WITH RECURSIVE x AS (
    (SELECT 1 AS n FROM remote('127.0.0.1', view(SELECT 1)))
    UNION ALL
    (SELECT n + 1 FROM x WHERE n < 3)
)
SELECT n FROM x ORDER BY n;
