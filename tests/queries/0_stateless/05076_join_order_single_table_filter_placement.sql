-- Tests that a join-order algorithm keeps single-table filter predicates that live in a join's ON
-- clause (e.g. `... JOIN t ON t.a = u.a AND t.b = 'x'`). The algorithm must attach such a predicate
-- at the join that introduces its relation. Two earlier placement bugs silently dropped the
-- predicate, letting extra rows through:
--   1. attaching only at two-relation joins dropped it when the relation was introduced against an
--      already-multi-relation subplan (e.g. `(t2 JOIN t3) JOIN t1`);
--   2. gating on fromLeft()/fromRight() dropped it for any filter on a relation whose id is >= 2,
--      because those helpers test relation ids 0 and 1 specifically.
-- For each shape we print the result with the 'greedy' algorithm (the reference, which places the
-- predicate correctly) and with 'dpsize'; the two must be identical row-for-row.
--
-- `query_plan_enable_optimizations = 0` is required to expose the bug and does NOT disable join
-- reordering (that is controlled by `query_plan_optimize_join_order_algorithm`). It disables the
-- general plan-optimization passes, notably filter push-down; with push-down enabled the
-- single-table filter is applied independently of the join and masks the dropped ON-clause
-- conjunct, so the wrong-result would not surface.
--
-- All joins are INNER: DPsize only reorders Inner joins (it bails out on outer joins), so an
-- outer-join shape would fall back instead of exercising this path.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t4 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (0, 'Join_1_Value_0'), (1, 'Join_1_Value_1'), (2, 'Join_1_Value_2');
INSERT INTO t2 VALUES (0, 'Join_2_Value_0'), (1, 'Join_2_Value_1'), (3, 'Join_2_Value_3');
INSERT INTO t3 VALUES (0, 'Join_3_Value_0'), (1, 'Join_3_Value_1'), (4, 'Join_3_Value_4');
INSERT INTO t4 VALUES (0, 'Join_4_Value_0'), (1, 'Join_4_Value_1'), (5, 'Join_4_Value_5');

SET enable_analyzer = 1;

-- Filter on a relation that is introduced last, against an already-joined subplan (relation id >= 2).
SELECT 'filter deep relation greedy:';
SELECT t2.id, t2.value, t3.id, t3.value, t1.id, t1.value
FROM t2 INNER JOIN t3 ON t2.id = t3.id
INNER JOIN t1 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0' ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_enable_optimizations = 0;

SELECT 'filter deep relation dpsize:';
SELECT t2.id, t2.value, t3.id, t3.value, t1.id, t1.value
FROM t2 INNER JOIN t3 ON t2.id = t3.id
INNER JOIN t1 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0' ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_enable_optimizations = 0;

-- Four-way all-inner chain with the filter on the last relation (id = 3).
SELECT 'four way filter last greedy:';
SELECT t1.id, t2.id, t3.id, t4.id, t4.value
FROM t1 INNER JOIN t2 ON t1.id = t2.id
INNER JOIN t3 ON t2.id = t3.id
INNER JOIN t4 ON t3.id = t4.id AND t4.value = 'Join_4_Value_0' ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_enable_optimizations = 0;

SELECT 'four way filter last dpsize:';
SELECT t1.id, t2.id, t3.id, t4.id, t4.value
FROM t1 INNER JOIN t2 ON t1.id = t2.id
INNER JOIN t3 ON t2.id = t3.id
INNER JOIN t4 ON t3.id = t4.id AND t4.value = 'Join_4_Value_0' ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_enable_optimizations = 0;

-- Two single-table filters on different relations.
SELECT 'two filters greedy:';
SELECT t1.id, t2.id, t3.id
FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0'
INNER JOIN t3 ON t2.id = t3.id AND t3.value = 'Join_3_Value_0' ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_enable_optimizations = 0;

SELECT 'two filters dpsize:';
SELECT t1.id, t2.id, t3.id
FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0'
INNER JOIN t3 ON t2.id = t3.id AND t3.value = 'Join_3_Value_0' ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_enable_optimizations = 0;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
