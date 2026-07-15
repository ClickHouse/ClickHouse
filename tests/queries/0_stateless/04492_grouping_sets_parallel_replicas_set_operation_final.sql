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
-- With the old analyzer, a non-zero automatic_parallel_replicas_mode disables parallel
-- replicas (InterpreterSelectQuery resets allow_experimental_parallel_reading_from_replicas),
-- so pin it to 0 to keep the ParallelReplicasUsedCount assertion below deterministic.
SET automatic_parallel_replicas_mode = 0;

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

-- When the set operation is a column-pruned subquery (non-empty required_result_column_names),
-- the constructor probes each child header first. That probe must also plan in its own settings
-- scope: if the FINAL branch's probe disables parallel replicas on the shared context, the
-- grouping-sets branch built afterwards silently loses parallel replicas. Assert the
-- grouping-sets branch still reads with parallel replicas here.
SELECT a FROM (
    SELECT sign AS a, count() AS c FROM t_gs_final GROUP BY GROUPING SETS ((sign), ('x'))
    UNION ALL
    SELECT sign AS a, count() AS c FROM t_gs_final FINAL GROUP BY sign
) ORDER BY a FORMAT Null SETTINGS log_comment = '04492_pruned_subquery_pr';

SYSTEM FLUSH LOGS query_log;

SELECT if(sum(ProfileEvents['ParallelReplicasUsedCount']) > 0, 'pruned subquery: pr used', 'pruned subquery: pr NOT used')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04492_pruned_subquery_pr'
  AND type = 'QueryFinish'
  AND is_initial_query
SETTINGS enable_parallel_replicas = 0;

SELECT 'ok';

DROP TABLE t_gs_final;
