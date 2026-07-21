-- Verify the topKThroughJoin optimization: ORDER BY + LIMIT pushed past a join
-- when the sort key only references columns from the side preserved by the join.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_r2;
DROP TABLE IF EXISTS t_r3;

CREATE TABLE t_l (id UInt64, k Int64, payload String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_r (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_l SELECT number, -toInt64(number), repeat('a', 8) FROM numbers(100000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(100000);

-- LEFT JOIN: sort key from preserved (left) side -> optimization applies.
SELECT 'left_join_top_k' AS label, count(*), max(lk), min(lk)
FROM (
    SELECT l.id AS lid, l.k AS lk, r.value AS rval
    FROM t_l AS l LEFT JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC
    LIMIT 10
);

-- Same query with optimization disabled: result must agree.
SELECT 'left_join_top_k_off' AS label, count(*), max(lk), min(lk)
FROM (
    SELECT l.id AS lid, l.k AS lk, r.value AS rval
    FROM t_l AS l LEFT JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC
    LIMIT 10
    SETTINGS query_plan_top_k_through_join = 0
);

-- INNER JOIN with selective right side: optimization must NOT fire (would be unsound).
-- t_r2 only contains even ids; the top-10 of t_l by k DESC are ids 0..9, only
-- 5 of which match. The full top-10 of the joined output is ids 0,2,...,18.
CREATE TABLE t_r2 (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_r2 SELECT number * 2, repeat('c', 8) FROM numbers(50000);

SELECT 'inner_join_correctness' AS label, count(*), max(lk), min(lk)
FROM (
    SELECT l.id AS lid, l.k AS lk, r.value AS rval
    FROM t_l AS l INNER JOIN t_r2 AS r ON r.id = l.id
    ORDER BY l.k DESC
    LIMIT 10
);

-- LEFT JOIN where each l-row matches multiple r-rows: outer LIMIT clips correctly.
CREATE TABLE t_r3 (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_r3 SELECT number % 1000, concat('v', toString(number)) FROM numbers(100000);

SELECT 'left_join_multiplied' AS label, count(*)
FROM (
    SELECT l.id AS lid, l.k AS lk, r.value AS rval
    FROM t_l AS l LEFT JOIN t_r3 AS r ON r.id = l.id
    ORDER BY l.k DESC
    LIMIT 10
);

-- Verify the plan: with the optimization on, a Limit + Sorting must appear inside
-- the join's left input subtree. With it off, the only Sort+Limit is at the top.
-- Optional optimizations that wrap the plan with extra steps (`BuildRuntimeFilter`,
-- `JoinLazyColumnsStep`, `LazilyReadFromMergeTree`) are disabled for the EXPLAIN
-- so the plan structure is deterministic across CI runs.
-- `query_plan_join_swap_table = false` disables the heuristic that may swap join
-- inputs (turning `LEFT JOIN` into `RIGHT JOIN` with sides reversed); without it
-- the plan structure depends on randomized settings.
-- `query_plan_max_limit_for_top_k_optimization = 0` removes the cap that the
-- stateless test runner randomizes - small values would prevent our optimization
-- from firing for `LIMIT 10`, again making the plan non-deterministic.
-- `query_plan_read_in_order_through_join = 0` prevents that pass from inserting
-- extra steps; this query's sort key (`k`) is not the storage's primary key
-- (`id`), so the deferral inside `topKThroughJoin` does not engage either way,
-- but disabling the pass keeps the plan stable against future changes there.
-- `enable_parallel_replicas = 0` keeps the plan local: the parallel-replicas
-- stateless test job otherwise wraps the local plan in a `Union` over a
-- `ReadFromRemoteParallelReplicas` step, which changes the EXPLAIN output.
EXPLAIN actions = 0
SELECT l.id, l.k, r.value
FROM t_l AS l LEFT JOIN t_r AS r ON r.id = l.id
ORDER BY l.k DESC
LIMIT 10
SETTINGS query_plan_top_k_through_join = 1, allow_experimental_analyzer = 1,
         enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
         query_plan_optimize_lazy_materialization = 0,
         query_plan_join_swap_table = false,
         query_plan_max_limit_for_top_k_optimization = 0,
         query_plan_read_in_order_through_join = 0,
         enable_parallel_replicas = 0;

EXPLAIN actions = 0
SELECT l.id, l.k, r.value
FROM t_l AS l LEFT JOIN t_r AS r ON r.id = l.id
ORDER BY l.k DESC
LIMIT 10
SETTINGS query_plan_top_k_through_join = 0, allow_experimental_analyzer = 1,
         enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
         query_plan_optimize_lazy_materialization = 0,
         query_plan_join_swap_table = false,
         query_plan_max_limit_for_top_k_optimization = 0,
         query_plan_read_in_order_through_join = 0,
         enable_parallel_replicas = 0;

DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_r2;
DROP TABLE t_r3;
