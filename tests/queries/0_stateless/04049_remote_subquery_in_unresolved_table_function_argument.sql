-- https://github.com/ClickHouse/ClickHouse/issues/83442
-- Scalar subquery inside an unresolved table function argument should not cause
-- a LOGICAL_ERROR about unexpected IDENTIFIER node in extractTableExpressions.

SELECT * FROM remote('localhost', view(SELECT 2 AS x), cityHash64(assumeNotNull(x + (SELECT 1))));
SELECT * FROM remote('localhost', view(SELECT 2 AS x), cityHash64(assumeNotNull((SELECT 1) + x)));

-- Re-analyzing an already resolved table function node (here because the USING key is resolved from
-- the SELECT list alias) must keep its `view` argument marked unresolved.
SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT 2 AS c0
FROM remote('localhost', view((SELECT 1 AS c1) INTERSECT DISTINCT SELECT 1 AS c1)) AS t0
INNER JOIN remote('localhost', view(SELECT 2 AS c0)) AS t1 USING (c0);

-- Only an argument that was already marked unresolved may stay skipped: a nested table function
-- that is legitimately resolved keeps its alias in the query tree.
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT 2 AS c0
    FROM remote('localhost', numbers(3)) AS t0
    INNER JOIN remote('localhost', view(SELECT 2 AS c0)) AS t1 USING (c0)
) WHERE explain ILIKE '%alias: __table%table_function_name: numbers%';
