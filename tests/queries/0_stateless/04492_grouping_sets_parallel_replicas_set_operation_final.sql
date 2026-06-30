-- Tags: no-parallel-replicas
-- A set operation where one branch uses GROUP BY GROUPING SETS over a parallel-replicas
-- read and the other branch uses FINAL must not abort the server. With the old
-- (non-analyzer) interpreter the FINAL branch disabled parallel replicas in a context
-- shared with the grouping-sets branch, which had already chosen WithMergeableState and a
-- MergingAggregatedStep with grouping sets, leaving its input header without __grouping_set.

DROP TABLE IF EXISTS t_gs_final;
CREATE TABLE t_gs_final (key Int32, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY key;
INSERT INTO t_gs_final SELECT number, if(number % 2, 1, -1) FROM numbers(20);

SET enable_analyzer = 0;
SET parallel_replicas_only_with_analyzer = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- UNION ALL: grouping-sets branch first, FINAL branch second.
SELECT NULL FROM t_gs_final GROUP BY GROUPING SETS ((sign), ('x'))
UNION ALL
SELECT NULL FROM t_gs_final FINAL GROUP BY sign
FORMAT Null;

-- INTERSECT, both branches use grouping sets, second branch uses FINAL.
SELECT DISTINCT NULL FROM t_gs_final GROUP BY GROUPING SETS ((sign), ('x'))
INTERSECT
SELECT DISTINCT NULL FROM t_gs_final FINAL GROUP BY GROUPING SETS ((sign), ('x'))
FORMAT Null;

-- The same set operation without grouping sets used to abort with
-- "Chunk info was not set for chunk in MergingAggregatedTransform"; it is fixed by the
-- same change (independent per-branch settings scope).
SELECT NULL FROM t_gs_final GROUP BY sign
UNION ALL
SELECT NULL FROM t_gs_final FINAL GROUP BY sign
FORMAT Null;

SELECT 'ok';

DROP TABLE t_gs_final;
