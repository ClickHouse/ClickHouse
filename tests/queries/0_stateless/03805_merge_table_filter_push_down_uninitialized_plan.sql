-- Test case for fix: QueryPlan was not initialized error when pushing down filter to Merge table

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS foo_merge;

CREATE TABLE t1 (Val UInt64) ENGINE = MergeTree ORDER BY Val;
CREATE TABLE t2 (Val UInt64) ENGINE = MergeTree ORDER BY Val;

-- Create a Merge table that matches only t1
CREATE TABLE foo_merge (Val UInt64) ENGINE = Merge(currentDatabase(), '^t1$');

-- This query used to cause "QueryPlan was not initialized" error
-- when filter push-down optimization tried to add filter to an uninitialized child plan
SELECT count() > 0 FROM viewExplain('EXPLAIN', '', (
    SELECT count() FROM foo_merge INNER JOIN t2 USING (Val)
    SETTINGS enable_join_runtime_filters = 1, parallel_replicas_local_plan = 1
));

DROP TABLE foo_merge;
DROP TABLE t2;
DROP TABLE t1;
