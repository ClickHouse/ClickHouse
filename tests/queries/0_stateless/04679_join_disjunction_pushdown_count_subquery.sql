-- Tags: no-parallel-replicas, no-random-settings
-- no-parallel-replicas: EXPLAIN / Prewhere differ with parallel replicas.
-- no-random-settings: `use_join_disjunctions_push_down` is randomized off.

-- Partial (disjunction) JOIN filter pushdown must remap `JoinStepLogical`
-- identifier aliases (`__table1.a`) to the child's physical column (`a`).
-- `count()` of `SELECT * … JOIN` drops unused JOIN-output names, so without
-- the remap `addFilterOnTop` threw `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left
(
    a Int32,
    b Int32
)
ENGINE = MergeTree
ORDER BY a;

CREATE TABLE t_right
(
    a Int32,
    b Int32
)
ENGINE = Memory;

INSERT INTO t_left VALUES (10, 1), (90, 2), (30, 3);
INSERT INTO t_right VALUES (60, 1), (5, 2), (30, 3);

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET use_join_disjunctions_push_down = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_use_nulls = 1;

SELECT count()
FROM
(
    SELECT *
    FROM t_left AS foo
    LEFT JOIN t_right AS bar ON foo.b = bar.b
    WHERE (foo.a < 40 AND bar.a > 50) OR (foo.a > 80 AND bar.a < 10)
);

DROP TABLE t_left;
DROP TABLE t_right;
