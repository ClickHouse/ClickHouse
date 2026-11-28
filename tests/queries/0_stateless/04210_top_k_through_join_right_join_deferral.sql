-- Verify that `topKThroughJoin` fires for `RIGHT JOIN` even when
-- `optimizeReadInOrder` (second-pass) cannot pick the optimization up.
--
-- The second-pass `optimizeReadInOrder` only traverses joins for `INNER`
-- and `LEFT` kinds with `ANY`/`ALL` strictness (via the left child). For a
-- `RIGHT` join the second pass does not propagate read-in-order through the
-- join at all, so `topKThroughJoin` must NOT defer to it - otherwise both
-- optimizations are silently disabled for `RIGHT JOIN`.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- `RIGHT JOIN` with sort key on the preserved (right) side, matching the
-- right table's primary key. Pre-fix, the deferral check fired and both
-- optimizations were skipped (only the outer Sort + Limit appeared, count=1).
-- Post-fix, the deferral is gated on `LEFT` so `topKThroughJoin` fires and
-- adds an inner Sort + Limit on the right preserved input (count=2).
SELECT 'right_join_topk' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l RIGHT JOIN t_r AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0
);

-- Result equivalence: `RIGHT JOIN` over identical 1..1000 ranges produces
-- 1000 matched rows; the top 10 by `r.k DESC` are 999..990.
-- `enable_parallel_replicas = 0`: under parallel replicas, `ORDER BY r.k DESC`
-- combined with `t_r ORDER BY k` engages read-in-order (`ReverseOrder`), and
-- the coordinator may receive announcements with a different mode from the
-- replica step that wraps the local plan, raising
-- `Coordination mode mismatch for stream ...`. This test verifies plan-level
-- result equivalence, not distributed execution.
SELECT 'right_join_result' AS label, count(*), max(rk), min(rk) FROM (
    SELECT r.k AS rk, r.value FROM t_l AS l RIGHT JOIN t_r AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

DROP TABLE t_l;
DROP TABLE t_r;
