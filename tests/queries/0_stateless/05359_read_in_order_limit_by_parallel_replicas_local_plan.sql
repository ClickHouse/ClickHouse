-- The local plan of a parallel-replicas read is re-optimized under the settings of its subquery
-- for every optimization that takes part in the read-in-order decision, including
-- `optimize_limit_by_in_order` and `query_plan_push_limit_by_into_sort`. Pin that `LIMIT BY`
-- queries return the same rows with a local parallel-replicas plan when the subquery overrides
-- these settings in either direction.

DROP TABLE IF EXISTS t_05355;

CREATE TABLE t_05355 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05355 SELECT intDiv(number, 3), number FROM numbers(300000);
OPTIMIZE TABLE t_05355 FINAL;

SET enable_analyzer = 1, optimize_read_in_order = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;

SET optimize_limit_by_in_order = 1, query_plan_push_limit_by_into_sort = 1;
SELECT count(), sum(k) FROM (SELECT k, v FROM t_05355 WHERE v % 2 = 0 LIMIT 1 BY k SETTINGS optimize_limit_by_in_order = 0);
SELECT count(), sum(k) FROM (SELECT k, v FROM t_05355 WHERE v % 2 = 0 ORDER BY k LIMIT 1 BY k SETTINGS query_plan_push_limit_by_into_sort = 0);
SELECT k, v FROM (SELECT k, v FROM t_05355 WHERE v % 2 = 0 ORDER BY k, v LIMIT 1 BY k SETTINGS query_plan_push_limit_by_into_sort = 0) ORDER BY k LIMIT 3;

SET optimize_limit_by_in_order = 0, query_plan_push_limit_by_into_sort = 0;
SELECT count(), sum(k) FROM (SELECT k, v FROM t_05355 WHERE v % 2 = 0 LIMIT 1 BY k SETTINGS optimize_limit_by_in_order = 1);
SELECT count(), sum(k) FROM (SELECT k, v FROM t_05355 WHERE v % 2 = 0 ORDER BY k LIMIT 1 BY k SETTINGS query_plan_push_limit_by_into_sort = 1);
SELECT k, v FROM (SELECT k, v FROM t_05355 WHERE v % 2 = 0 ORDER BY k, v LIMIT 1 BY k SETTINGS query_plan_push_limit_by_into_sort = 1) ORDER BY k LIMIT 3;

DROP TABLE t_05355;
