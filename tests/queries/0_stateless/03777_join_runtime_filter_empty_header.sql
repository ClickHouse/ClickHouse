SET analyzer_compatibility_join_using_top_level_identifier = 1;
SET enable_join_runtime_filters = 1;

EXPLAIN header=1
SELECT 1 AS c0 FROM (SELECT 1 AS c1) t0 JOIN (SELECT 1 AS c0) t1 USING (c0);

SELECT 1 AS c0 FROM (SELECT 1 AS c1) t0 JOIN (SELECT 1 AS c0) t1 USING (c0);
