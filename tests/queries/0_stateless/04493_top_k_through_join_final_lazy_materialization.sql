-- Regression test for the interaction between `query_plan_top_k_through_join` and
-- lazy materialization under `FINAL`.
--
-- `topKThroughJoin` pushes `Sort + Limit` onto the preserved side of the join,
-- placing the inserted `SortingStep` directly above the `WHERE` `FilterStep` that
-- reads from a `FINAL` `ReplacingMergeTree`. Lazy materialization then fires on
-- that inserted `Limit <- Sort <- Filter <- ReadFromMergeTree` island and splits
-- the filter into a main half (kept before the `LIMIT`) and a lazy half (applied
-- after re-reading the deferred columns). The main half removes the filter column,
-- but the lazy half still carried it as a pass-through, and because it was the
-- last lazy step it was never cleaned up by `removeDanglingNodes` - so executing
-- the plan threw `Not found column equals(...) ... (NOT_FOUND_COLUMN_IN_BLOCK)`.
--
-- The trigger requires all of: preserved side read with `FINAL`, a `WHERE` on the
-- preserved table, `ORDER BY <preserved column> LIMIT n`, and a select-list-only
-- column that lazy materialization defers.
--
-- See PR #104268 (introduced `topKThroughJoin`) and #93186/#93316 (prior top-K
-- header fix).

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;
SET query_plan_optimize_lazy_materialization = 1;

DROP TABLE IF EXISTS t_l_lazy;
DROP TABLE IF EXISTS t_r_lazy;

CREATE TABLE t_l_lazy (id String, k String, ts DateTime64(3), v String) ENGINE = ReplacingMergeTree ORDER BY id;
CREATE TABLE t_r_lazy (id String, name String) ENGINE = ReplacingMergeTree ORDER BY id;

INSERT INTO t_l_lazy SELECT toString(number), if(number % 3 = 0, 'k1', 'k2'), toDateTime64(1700000000 + number, 3), concat('v', toString(number)) FROM numbers(50);
INSERT INTO t_r_lazy SELECT toString(number), concat('nm', toString(number)) FROM numbers(30);

-- The minimal reproduction: single matching row, `v` referenced only in the select
-- list (deferred by lazy materialization), `k` referenced only in the `WHERE`.
DROP TABLE IF EXISTS t_l_min;
DROP TABLE IF EXISTS t_r_min;
CREATE TABLE t_l_min (id String, k String, ts DateTime64(3), v String) ENGINE = ReplacingMergeTree ORDER BY id;
CREATE TABLE t_r_min (id String, name String) ENGINE = ReplacingMergeTree ORDER BY id;
INSERT INTO t_l_min VALUES ('a', 'k1', '2026-06-23 08:10:00', 'x');
INSERT INTO t_r_min VALUES ('a', 'nm');

SELECT 'min_left_final' AS label, e.id, e.v
FROM t_l_min AS e FINAL
LEFT JOIN t_r_min AS tc ON tc.id = e.id
WHERE e.k = 'k1'
ORDER BY e.ts DESC
LIMIT 10;

-- LEFT JOIN with FINAL on the preserved (left) side. Result must be identical with
-- the optimization enabled (default) and disabled.
SELECT 'left_final_on' AS label, e.id, e.v
FROM t_l_lazy AS e FINAL
LEFT JOIN t_r_lazy AS tc ON tc.id = e.id
WHERE e.k = 'k1'
ORDER BY e.ts DESC
LIMIT 5;

SELECT 'left_final_off' AS label, e.id, e.v
FROM t_l_lazy AS e FINAL
LEFT JOIN t_r_lazy AS tc ON tc.id = e.id
WHERE e.k = 'k1'
ORDER BY e.ts DESC
LIMIT 5
SETTINGS query_plan_top_k_through_join = 0;

-- RIGHT JOIN with FINAL on the preserved (right) side.
SELECT 'right_final_on' AS label, e.id, e.v
FROM t_r_lazy AS tc RIGHT JOIN t_l_lazy AS e FINAL ON tc.id = e.id
WHERE e.k = 'k1'
ORDER BY e.ts DESC
LIMIT 5;

SELECT 'right_final_off' AS label, e.id, e.v
FROM t_r_lazy AS tc RIGHT JOIN t_l_lazy AS e FINAL ON tc.id = e.id
WHERE e.k = 'k1'
ORDER BY e.ts DESC
LIMIT 5
SETTINGS query_plan_top_k_through_join = 0;

-- Several deferred columns and a multi-condition `WHERE`.
SELECT 'left_final_multi_on' AS label, e.id, e.v, e.k
FROM t_l_lazy AS e FINAL
LEFT JOIN t_r_lazy AS tc ON tc.id = e.id
WHERE e.k = 'k1' AND e.v != 'zzz'
ORDER BY e.ts DESC
LIMIT 7;

SELECT 'left_final_multi_off' AS label, e.id, e.v, e.k
FROM t_l_lazy AS e FINAL
LEFT JOIN t_r_lazy AS tc ON tc.id = e.id
WHERE e.k = 'k1' AND e.v != 'zzz'
ORDER BY e.ts DESC
LIMIT 7
SETTINGS query_plan_top_k_through_join = 0;

DROP TABLE t_l_lazy;
DROP TABLE t_r_lazy;
DROP TABLE t_l_min;
DROP TABLE t_r_min;
