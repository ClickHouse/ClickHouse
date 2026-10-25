-- Verify the topKThroughJoin optimization is correctly skipped in cases where it
-- would change query semantics:
--   * `LEFT SEMI` / `LEFT ANTI` / `RIGHT SEMI` / `RIGHT ANTI` - preserved-side rows
--     can be filtered out by the strictness, breaking the soundness invariant.
--   * `LIMIT WITH TOTALS` - the upper LimitStep has `alwaysReadTillEnd` set, and
--     truncating the preserved input would understate the totals.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (id UInt64, k Int64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_r (id UInt64) ENGINE = MergeTree() ORDER BY id;

-- Top-10 of t_l by k DESC are ids 0..9. t_r contains only odd ids, so
-- LEFT SEMI/ANTI must filter t_l accordingly. If the optimization fires,
-- it would prune t_l to those top-10 ids before the SEMI/ANTI filter,
-- changing the output.
INSERT INTO t_l SELECT number, -toInt64(number) FROM numbers(100);
INSERT INTO t_r SELECT number * 2 + 1 FROM numbers(50);

SELECT 'left_semi' AS label, count(*), max(k), min(k) FROM (
    SELECT l.id, l.k FROM t_l AS l LEFT SEMI JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC LIMIT 10
);

SELECT 'left_anti' AS label, count(*), max(k), min(k) FROM (
    SELECT l.id, l.k FROM t_l AS l LEFT ANTI JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC LIMIT 10
);

-- Same shape for RIGHT SEMI/ANTI.
SELECT 'right_semi' AS label, count(*) FROM (
    SELECT r.id FROM t_l AS l RIGHT SEMI JOIN t_r AS r ON r.id = l.id
    ORDER BY r.id DESC LIMIT 10
);

SELECT 'right_anti' AS label, count(*) FROM (
    SELECT r.id FROM t_l AS l RIGHT ANTI JOIN t_r AS r ON r.id = l.id
    ORDER BY r.id DESC LIMIT 10
);

-- The plan for `LEFT SEMI` must NOT contain a Limit + Sorting on the preserved (left)
-- input subtree. Confirm there is exactly one Sort + Limit pair, the outer one.
-- `enable_parallel_replicas = 0` keeps the plan local: the parallel-replicas
-- stateless test job otherwise wraps the local plan in a `Union` over a
-- `ReadFromRemoteParallelReplicas` step, which adds a second `Sorting` /
-- `Limit` and inflates the count.
SELECT 'left_semi_plan_sort_count' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.id, l.k FROM t_l AS l LEFT SEMI JOIN t_r AS r ON r.id = l.id
    ORDER BY l.k DESC LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

-- WITH TOTALS sets `alwaysReadTillEnd` on the LimitStep; the optimization must be
-- skipped so the totals are computed over the full join output.
SELECT 'limit_with_totals_plan_sort_count' AS label, countIf(explain LIKE '%Sorting%') AS sort_count
FROM ( EXPLAIN actions = 0
    SELECT l.id, l.k, count() AS c FROM t_l AS l LEFT JOIN t_r AS r ON r.id = l.id
    GROUP BY l.id, l.k WITH TOTALS
    ORDER BY l.k DESC LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

-- Alias shadowing: the user-visible alias `k` is a *computed* expression
-- (`l.k + r.bonus`), so `ORDER BY k` cannot be pushed onto the preserved (left) input
-- because `l.k` alone is not the sort key. The pass-through check must reject this.
DROP TABLE IF EXISTS t_lb;
DROP TABLE IF EXISTS t_rb;
CREATE TABLE t_lb (id UInt64, k Int64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_rb (id UInt64, bonus Int64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_lb VALUES (1, 100), (2, 90);
INSERT INTO t_rb VALUES (1, 0), (2, 1000);

-- Correct top-1 by computed `k` is id=2 (90 + 1000 = 1090). If the optimization
-- were to fire and push `ORDER BY l.k` onto the left input, only id=1 (l.k = 100)
-- would survive, giving the wrong answer.
SELECT 'alias_shadow_correctness' AS label, id, k FROM (
    SELECT l.id AS id, l.k + r.bonus AS k
    FROM t_lb AS l LEFT JOIN t_rb AS r ON r.id = l.id
    ORDER BY k DESC LIMIT 1
);

DROP TABLE t_lb;
DROP TABLE t_rb;
DROP TABLE t_l;
DROP TABLE t_r;
