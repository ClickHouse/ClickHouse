-- Regression test for the ParallelReplicasSplitStep header when it is pushed above a
-- header-changing step. The split step is a pass-through marker; when the optimizer moves it
-- above an ExpressionStep or FilterStep its header must follow the new child's output. If it
-- keeps the stale pre-step header (e.g. `a` instead of `a % 7`) plan building fails with
-- THERE_IS_NO_COLUMN. Results must match non-parallel execution.
--
-- The queries are deliberately non-aggregating: an AggregatingStep above the split rebuilds it
-- with a fresh (correct) header and would mask the bug. Single-row PK filters keep the output
-- deterministic without ORDER BY.

DROP TABLE IF EXISTS t_pr_split_header;

CREATE TABLE t_pr_split_header (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_split_header SELECT number, number % 10 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
-- Pin the manual mode: otherwise CI's randomized automatic_parallel_replicas_mode can cost-decide
-- against parallel replicas, so the plan-based split (and the header code path) does not engage.
SET automatic_parallel_replicas_mode = 0;

-- Expression above the split: the marker is pushed above `a % 7`.
SELECT a % 7 AS x FROM t_pr_split_header WHERE a = 12345;
-- Filter (on non-PK `b`) + expression above the split.
SELECT a % 7 AS x FROM t_pr_split_header WHERE a = 12345 AND b = 5;
-- Filter above the split (no expression).
SELECT a AS x FROM t_pr_split_header WHERE a = 12345 AND b = 5;

DROP TABLE t_pr_split_header;
