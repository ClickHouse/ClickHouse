-- A tiny Cascades task budget must fail closed with a clear exception instead of
-- building a plan from a partial memo (which would be non-minimal, or fail
-- confusingly deep in buildBestPlan). The budget is overridable for tests via the
-- `_internal_cascades_task_limit` query parameter; the default is large.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_task_budget;
CREATE TABLE t_task_budget (k UInt64, x UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_task_budget SELECT number % 100, number FROM numbers(1000);

SELECT '-- default budget: the query optimizes and runs';
SELECT k, sum(x) FROM t_task_budget GROUP BY k ORDER BY k LIMIT 3
SETTINGS distributed_plan_execute_locally = 1;

SELECT '-- task budget of 1 cannot finish: fail closed';
SET param__internal_cascades_task_limit = 1;
SELECT k, sum(x) FROM t_task_budget GROUP BY k ORDER BY k; -- { serverError SUPPORT_IS_DISABLED }

-- Raise the override above the built-in cap; it is clamped back to the cap, so the query runs.
SET param__internal_cascades_task_limit = 100000000;
SELECT '-- an over-large override is clamped to the built-in cap, so the query still runs';
SELECT k, sum(x) FROM t_task_budget GROUP BY k ORDER BY k LIMIT 3
SETTINGS distributed_plan_execute_locally = 1;

DROP TABLE t_task_budget;
