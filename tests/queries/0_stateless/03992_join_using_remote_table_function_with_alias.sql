SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

-- Without `remote`, the join works fine.
SELECT 0 AS number FROM numbers(1) AS a JOIN numbers(1) AS b USING (number);

-- Previously this caused a LOGICAL_ERROR because the nested table function `numbers` inside
-- `remote` was already resolved to a `TableFunctionNode` during the first analysis pass,
-- but when re-analyzed in the synthetic subquery created for USING alias resolution,
-- it was not recognized as a table function argument and fell through to `resolveExpressionNode`.
SELECT 0 AS number FROM remote('127.0.0.1', numbers(1)) AS a JOIN numbers(1) AS b USING (number);
SELECT '' AS c1 FROM remote('127.0.0.1', `null`('c1 String')) AS a JOIN `null`('c1 String') AS b USING (c1);
