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

-- Lazy materialization must fire for all the queries below, otherwise they assert nothing.
-- Both the local and the distributed plan are checked: the bug only shows under
-- `make_distributed_plan`, so a guard covering the local plan alone could pass vacuously.
SELECT 'lazy materialization applied:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4);

SELECT 'lazy materialization applied (distributed):', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4
      SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1);

-- Lazy materialization replaces the `LimitStep` node with a `JoinLazyColumnsStep` subplan whose
-- header follows the main/lazy split, not the projection. `make_distributed_plan` rebuilds the
-- plan bottom-up and re-derives every header, so a replacement that lost the replaced node's
-- header order raised `LOGICAL_ERROR` `Cannot add step Expression to QueryPlan because it has
-- incompatible header with root step JoinLazyColumnsStep`.
SELECT v, ver, k FROM t_lm_header_order FINAL PREWHERE k > 0 LIMIT 4
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

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

DROP TABLE t_lm_header_order;
