-- Regression test: window function in GROUP BY key should not cause a logical error
-- in canRemoveConstantFromGroupByKey when the window function gets constant-folded
-- and the query uses a distributed/remote table.
SET enable_analyzer = 1;
SELECT count() FROM remote('127.0.0.1', numbers(10)) GROUP BY and(isNull(9), row_number(1) OVER (ORDER BY 1 ASC));
