-- Tags: no-fasttest
-- Regression test: recursive CTE with remote() + view() used to segfault
-- because isStorageUsedInTree tried to call getStorageID() on an unresolved
-- view() TableFunctionNode whose storage was null.

SET enable_analyzer=1;

WITH RECURSIVE x AS (
    (SELECT 1 FROM remote('127.0.0.1', view(SELECT 1)))
    UNION ALL
    (SELECT 1)
)
SELECT 1 FROM x;
