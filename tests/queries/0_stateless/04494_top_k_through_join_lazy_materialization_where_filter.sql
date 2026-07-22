-- Regression test for the same root cause as
-- `04493_top_k_through_join_final_lazy_materialization`, covering the shapes
-- reported in issue #111252 where the bug fires WITHOUT `FINAL`: it only needs
-- the `WHERE` filter to stay a `FilterStep` (i.e. NOT moved to `PREWHERE`).
--
-- `topKThroughJoin` pushes `Sort + Limit` onto the preserved side of the join,
-- forming a `Limit <- Sort <- Filter <- ReadFromMergeTree` island. Lazy
-- materialization splits that filter; the main half removes the filter column,
-- but `ActionsDAG::split` left it as a dangling pass-through in the lazy half
-- (never cleaned because the filter's lazy half is the last lazy step), so the
-- plan threw `Not found column <predicate> ... (NOT_FOUND_COLUMN_IN_BLOCK)`.
--
-- The filter stays in `WHERE` either because the user set
-- `optimize_move_to_prewhere = 0`, or - under fully default settings - because a
-- tautological conjunct (`... AND CAST(1 AS LowCardinality(Nullable(UInt8)))`)
-- blocks the `PREWHERE` move. The predicate itself is arbitrary: `IN (...)`,
-- a plain comparison, `LIKE`, and `OR` all lose their input column the same way.
--
-- Each result must be identical with lazy materialization enabled (default) and
-- disabled. See #109210 and #111252, and PR #104268 (introduced `topKThroughJoin`).

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_lzm;
DROP TABLE IF EXISTS t_lzt;

CREATE TABLE t_lzm (id UInt32, v Int64, s String) ENGINE = MergeTree ORDER BY (toStartOfHour(toDateTime(id)), id);
INSERT INTO t_lzm SELECT number, number - 300, toString(number % 10) FROM numbers(2000);
CREATE TABLE t_lzt (id UInt32, body String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lzt SELECT number, toString(number) FROM numbers(500);

-- IN-set predicate on a LEFT JOIN, filter kept in WHERE via optimize_move_to_prewhere = 0.
SELECT 'in_set_mtp0_on' AS label, l.v
FROM t_lzm AS l LEFT JOIN t_lzt AS r ON l.id = r.id
WHERE l.s IN ('5', '1') ORDER BY l.v LIMIT 2
SETTINGS optimize_move_to_prewhere = 0;

SELECT 'in_set_mtp0_off' AS label, l.v
FROM t_lzm AS l LEFT JOIN t_lzt AS r ON l.id = r.id
WHERE l.s IN ('5', '1') ORDER BY l.v LIMIT 2
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 0;

-- IN-set predicate under default settings, with a tautological conjunct that
-- blocks the PREWHERE move (so the filter stays in WHERE without touching
-- optimize_move_to_prewhere).
SELECT 'in_set_default_on' AS label, l.v
FROM t_lzm AS l LEFT JOIN t_lzt AS r ON l.id = r.id
WHERE l.s IN ('5', '1') AND CAST(1 AS LowCardinality(Nullable(UInt8))) ORDER BY l.v LIMIT 2
SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT 'in_set_default_off' AS label, l.v
FROM t_lzm AS l LEFT JOIN t_lzt AS r ON l.id = r.id
WHERE l.s IN ('5', '1') AND CAST(1 AS LowCardinality(Nullable(UInt8))) ORDER BY l.v LIMIT 2
SETTINGS allow_suspicious_low_cardinality_types = 1, query_plan_optimize_lazy_materialization = 0;

-- LIKE + OR multi-condition WHERE on a LEFT JOIN.
SELECT 'like_or_mtp0_on' AS label, l.v, l.s
FROM t_lzm AS l LEFT JOIN t_lzt AS r ON l.id = r.id
WHERE l.s LIKE '5' OR l.s = '1' ORDER BY l.v LIMIT 3
SETTINGS optimize_move_to_prewhere = 0;

SELECT 'like_or_mtp0_off' AS label, l.v, l.s
FROM t_lzm AS l LEFT JOIN t_lzt AS r ON l.id = r.id
WHERE l.s LIKE '5' OR l.s = '1' ORDER BY l.v LIMIT 3
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 0;

-- Plain comparison on a RIGHT JOIN ... USING with the filter on the preserved
-- (right) table, no FINAL.
DROP TABLE IF EXISTS t_l_using;
DROP TABLE IF EXISTS t_r_using;
CREATE TABLE t_l_using (v Int64, x String) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_l_using SELECT number, toString(number) FROM numbers(100);
-- `ts` is `DateTime('UTC')` so its rendered value does not depend on the
-- server/session timezone (a plain `DateTime` renders in the server timezone and
-- makes the reference non-deterministic across CI runners).
CREATE TABLE t_r_using (v Int64, id UInt32, ts DateTime('UTC')) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_r_using SELECT number, number, toDateTime(1700000000 + number, 'UTC') FROM numbers(100);

SELECT 'right_using_mtp0_on' AS label, r.ts
FROM t_l_using AS l RIGHT JOIN t_r_using AS r USING (v)
WHERE r.id != 50 ORDER BY ALL LIMIT 5
SETTINGS optimize_move_to_prewhere = 0;

SELECT 'right_using_mtp0_off' AS label, r.ts
FROM t_l_using AS l RIGHT JOIN t_r_using AS r USING (v)
WHERE r.id != 50 ORDER BY ALL LIMIT 5
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 0;

DROP TABLE t_lzm;
DROP TABLE t_lzt;
DROP TABLE t_l_using;
DROP TABLE t_r_using;
