-- Tags: no-fasttest
-- Reason: COLLATE requires ICU, which is disabled in the Fast Test build.
--
-- Regression test for the deferral check in `topKThroughJoin`: when the sort
-- column matches the storage's primary key by name but `optimizeReadInOrder`
-- cannot actually satisfy the `SortingStep` (here: `ORDER BY ... COLLATE`),
-- the optimization must not silently defer. Otherwise the user gets neither
-- read-in-order nor the explicit `Sort + Limit` on the preserved input, and
-- we fall back to a full join + full sort.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

-- `s` is the storage primary key, so a name-only deferral check would defer
-- here. `optimizeReadInOrder` cannot honor `ORDER BY s COLLATE 'en'` from a
-- MergeTree ordered by `s` without a collator, so the deferral would disable
-- both optimizations.
CREATE TABLE t_l (s String, payload String) ENGINE = MergeTree() ORDER BY s;
CREATE TABLE t_r (s String, value String) ENGINE = MergeTree() ORDER BY s;

INSERT INTO t_l SELECT toString(number), repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT toString(number), repeat('b', 8) FROM numbers(1000);

-- The explicit `Sort + Limit` on the preserved input is expected: two `Sorting`
-- and two `Limit` steps in the plan (outer pair + injected pair).
SELECT 'collation' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.s, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.s = l.s
    ORDER BY l.s DESC COLLATE 'en' LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0
);

-- Result equivalence: same query with the optimization disabled produces the
-- same top-10 by collated `s`.
SELECT 'result_on' AS label, count(*), max(s), min(s) FROM (
    SELECT l.s AS s, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.s = l.s
    ORDER BY l.s DESC COLLATE 'en' LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

SELECT 'result_off' AS label, count(*), max(s), min(s) FROM (
    SELECT l.s AS s, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.s = l.s
    ORDER BY l.s DESC COLLATE 'en' LIMIT 10
    SETTINGS query_plan_top_k_through_join = 0, enable_parallel_replicas = 0
);

DROP TABLE t_l;
DROP TABLE t_r;
