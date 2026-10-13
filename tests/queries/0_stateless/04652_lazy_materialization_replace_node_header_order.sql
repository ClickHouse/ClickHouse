-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local query plan.

DROP TABLE IF EXISTS t_lm_header_order;

CREATE TABLE t_lm_header_order (k Int32, v UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_lm_header_order VALUES (1, 100, 1), (2, 200, 1), (3, 300, 1), (4, 400, 1);

-- The test relies on settings that are randomized by the test runner: pin them.
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;

-- Lazy materialization must fire for the local plan, otherwise the queries below assert nothing.
SELECT 'lazy materialization applied:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4);

-- Lazy materialization must not fire while the initiator builds a distributed plan: its steps
-- (`JoinLazyColumnsStep`, `LazilyReadFromMergeTree`) are not serializable, and even an
-- exchange-free plan like this one ships as one serialized fragment. The executor re-applies
-- lazy materialization inside the fragment when it re-optimizes it with `make_distributed_plan = 0`.
SELECT 'no lazy step in coordinator plan (distributed):', countIf(explain LIKE '%LazilyReadFromMergeTree%') = 0
FROM (EXPLAIN SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4
      SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1);

-- A `FINAL PREWHERE ... LIMIT` plan gets no exchange under `make_distributed_plan`, so it ships
-- as one serialized fragment. These queries pin that this shape executes through the distributed
-- machinery without an exception and returns the same rows as the local plan.
SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, log_comment = '04652_distributed';


SELECT v, k FROM t_lm_header_order FINAL PREWHERE k > 2 LIMIT 4
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT ver, v, k FROM t_lm_header_order FINAL PREWHERE k = NULL LIMIT 4
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- The distributed plan must return exactly what the local plan returns.
SELECT 'distributed == local:', (
    SELECT groupArray((v, ver, k)) FROM (
        SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4
        SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1))
    = (
    SELECT groupArray((v, ver, k)) FROM (
        SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4));

-- Restoring the header must not change what lazy materialization returns.
SELECT 'lazy == not lazy:', (
    SELECT groupArray((v, ver, k)) FROM (
        SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4))
    = (
    SELECT groupArray((v, ver, k)) FROM (
        SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4
        SETTINGS query_plan_optimize_lazy_materialization = 0));

SYSTEM FLUSH LOGS query_log;

-- `DistributedPlanLocalExecution` is set on the query's own entry when the distributed plan ran
-- through the in-process executor, proving the query was not silently planned as a local one.
SELECT 'distributed plan executed:', max(ProfileEvents['DistributedPlanLocalExecution']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = '04652_distributed';

DROP TABLE t_lm_header_order;
