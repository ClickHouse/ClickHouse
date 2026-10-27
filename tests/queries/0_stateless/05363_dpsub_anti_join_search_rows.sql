-- An `ANTI` join keeps at most the rows of its preserved side, and DPsub costs it so, like the other
-- solvers: its cost path and the shared one count a `SEMI`, `ANTI` or `ANY` join by its strictness.
-- Here no input has statistics, so the costs use the inputs' row bounds. Without this DPsub costed
-- the anti join as a product of its inputs and applied it to the large filtered `a` first; with it,
-- `a` joins the selective `c` first. Only the join labels are checked: the rest of the plan varies
-- with settings the runner randomizes.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
-- The order is chosen by the solver only when join reordering is on; the runner randomizes its limit.
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET use_statistics = 0;

DROP TABLE IF EXISTS t_ds_a;
DROP TABLE IF EXISTS t_ds_b;
DROP TABLE IF EXISTS t_ds_c;

CREATE TABLE t_ds_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ds_b (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ds_c (k UInt64, z UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_ds_a SELECT number, number FROM numbers(100000);
INSERT INTO t_ds_b SELECT number, number FROM numbers(1000);
INSERT INTO t_ds_c SELECT number * 7, number FROM numbers(20000);

SELECT replaceRegexpAll(explain, '^[^a-z(]*', '')
FROM (
    EXPLAIN SELECT count() FROM t_ds_a AS a LEFT ANTI JOIN t_ds_b AS b ON a.k = b.k JOIN t_ds_c AS c ON a.k = c.k
    WHERE a.v % 3 = 1 AND c.z % 7 = 1
)
WHERE explain LIKE '%⋈%' OR explain LIKE '%⋊%' OR explain LIKE '%⋉%';

DROP TABLE t_ds_a;
DROP TABLE t_ds_b;
DROP TABLE t_ds_c;
