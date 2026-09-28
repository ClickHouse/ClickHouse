-- Tags: no-parallel, no-parallel-replicas

DROP TABLE IF EXISTS t_row_policy_or;
CREATE TABLE t_row_policy_or (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_row_policy_or SELECT number, toString(number) FROM numbers(10);

DROP ROW POLICY IF EXISTS 04098_p1 ON t_row_policy_or;
DROP ROW POLICY IF EXISTS 04098_p2 ON t_row_policy_or;
DROP ROW POLICY IF EXISTS 04098_p3 ON t_row_policy_or;

CREATE ROW POLICY 04098_p1 ON t_row_policy_or USING id = 1 AS permissive TO ALL;
CREATE ROW POLICY 04098_p2 ON t_row_policy_or USING id = 3 AS permissive TO ALL;
CREATE ROW POLICY 04098_p3 ON t_row_policy_or USING id = 5 AS permissive TO ALL;

-- With the old analyzer the OR chain is converted to IN by LogicalExpressionsOptimizer.
-- With the new analyzer the same should happen via LogicalExpressionOptimizerPass
-- applied to the row policy filter in buildFilterInfo.

SET enable_analyzer = 0;
SELECT 'old analyzer';
SELECT explain FROM (EXPLAIN actions = 1 SELECT id FROM t_row_policy_or)
WHERE explain LIKE '%Row level filter column:%';

SET enable_analyzer = 1;
SELECT 'new analyzer';
SELECT replaceRegexpOne(explain, '__set_UInt64_\\d+_\\d+', '__set_UInt64_')
FROM (EXPLAIN actions = 1 SELECT id FROM t_row_policy_or)
WHERE explain LIKE '%Row level filter column:%';

SELECT 'result';
SELECT id FROM t_row_policy_or ORDER BY id;

DROP ROW POLICY 04098_p1 ON t_row_policy_or;
DROP ROW POLICY 04098_p2 ON t_row_policy_or;
DROP ROW POLICY 04098_p3 ON t_row_policy_or;
DROP TABLE t_row_policy_or;
