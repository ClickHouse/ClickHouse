-- Enabling a join-order conflict detector must not make a plannable query unplannable. A join whose
-- ON clause references relations on only one input side is a degenerate predicate / cross product
-- (paper "On the Correct and Complete Enumeration of the Core Search Space", Section 6.2): it
-- contributes no binary join edge, so DPsub's enumerator must still be able to connect its two
-- subtrees. Regression test for https://github.com/ClickHouse/ClickHouse/issues/118938, where
-- `t1 LEFT JOIN t2 ON t2.a = 5 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k` planned fine with no
-- reordering, with greedy, and with plain dpsub, but returned Code 717 as soon as a detector was on.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t3 (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (1, 5), (2, 7);
INSERT INTO t3 VALUES (0), (1), (2), (3);

SET enable_analyzer = 1, query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10;

-- Degenerate predicate on the null-supplying side (t2.a = 5). All four configurations must agree.
SELECT 'noopt', t1.k, t2.k, t3.k FROM t1 LEFT JOIN t2 ON t2.a = 5 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_limit = 0;
SELECT 'greedy', t1.k, t2.k, t3.k FROM t1 LEFT JOIN t2 ON t2.a = 5 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';
SELECT 'dpsub', t1.k, t2.k, t3.k FROM t1 LEFT JOIN t2 ON t2.a = 5 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';
SELECT 'cdc', t1.k, t2.k, t3.k FROM t1 LEFT JOIN t2 ON t2.a = 5 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;
SELECT 'cda', t1.k, t2.k, t3.k FROM t1 LEFT JOIN t2 ON t2.a = 5 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;

-- A pure cross product feeding an inner join must plan under a detector too.
SELECT 'cross cdc', count() FROM t1 CROSS JOIN t2 JOIN t3 ON t1.k = t3.k AND t2.k = t3.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
