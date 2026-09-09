-- Enabling a join-order conflict detector
-- (query_plan_optimize_join_order_use_conflict_detector_a/_c) must not change the strictness of a
-- join: ANY/SEMI/ANTI joins must stay ANY/SEMI/ANTI and never degrade to ALL. Regression test for
-- https://github.com/ClickHouse/ClickHouse/issues/118520, where the detector silently re-stamped the
-- reordered joins as ALL, multiplying rows. The counts below must be identical with the detector off
-- and on, and across the greedy (default) and dpsub join-order algorithms.

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number FROM numbers(4);      -- 0, 1, 2, 3
INSERT INTO r SELECT 1 FROM numbers(3);           -- key 1, three times

SET enable_analyzer = 1;

-- ANY LEFT: one row per left row regardless of how many right rows match -> 4.
SELECT 'ANY  greedy cd_a', count() FROM l ANY LEFT JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'ANY  greedy cd_c', count() FROM l ANY LEFT JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_use_conflict_detector_c = 1;
SELECT 'ANY  dpsub  cd_a', count() FROM l ANY LEFT JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'ANY  dpsub  cd_c', count() FROM l ANY LEFT JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;

-- SEMI LEFT: left rows that have a match -> 1 (only key 1).
SELECT 'SEMI greedy cd_a', count() FROM l LEFT SEMI JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'SEMI dpsub  cd_c', count() FROM l LEFT SEMI JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;

-- ANTI LEFT: left rows without a match -> 3 (keys 0, 2, 3).
SELECT 'ANTI greedy cd_a', count() FROM l LEFT ANTI JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'ANTI dpsub  cd_c', count() FROM l LEFT ANTI JOIN r ON l.k = r.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;

DROP TABLE l;
DROP TABLE r;

-- A CD-reordered SEMI/ANTI join can be nested under an inner top join, not just at the top. The
-- strictness re-stamp must not flatten such a nested join to a plain join (it did in an early version
-- of the fix, turning LEFT ANTI into LEFT and multiplying rows). Here the top join is INNER, so its
-- graph strictness is ALL, yet the nested SEMI/ANTI must keep its own strictness under dpsub + CD.
DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
CREATE TABLE a (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE b (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE c (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO a SELECT number FROM numbers(6);   -- 0,1,2,3,4,5
INSERT INTO b VALUES (1), (3);
INSERT INTO c VALUES (0), (1), (2), (4);

-- a LEFT ANTI b -> {0,2,4,5}; INNER JOIN c ON a.k = c.k -> {0,2,4} = 3.
SELECT 'nested ANTI dpsub cd_c', count() FROM a LEFT ANTI JOIN b ON a.k = b.k INNER JOIN c ON a.k = c.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;
-- a LEFT SEMI b -> {1,3}; INNER JOIN c ON a.k = c.k -> {1} = 1.
SELECT 'nested SEMI dpsub cd_a', count() FROM a LEFT SEMI JOIN b ON a.k = b.k INNER JOIN c ON a.k = c.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
