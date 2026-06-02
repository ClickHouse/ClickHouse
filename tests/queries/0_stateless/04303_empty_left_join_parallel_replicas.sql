-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/89166
--
-- An empty-left INNER JOIN with parallel replicas used to produce a plan with
-- two nested `JoinLogical` steps: an outer "stub" join inserted by the
-- per-table-expression visitor and an inner join coming from the right leaf's
-- parallel-replicas plan (which received the full join query tree).
-- The new logical-join reorder pass then folded both into a single `QueryGraph`,
-- saw `__table1.id` appearing on both sides, and threw
--   LOGICAL_ERROR: Left and right columns have same names: [__table1.id], [__table1.id]
-- The fix makes the leftmost-leaf-drives-parallel-replicas contract explicit:
-- only the leftmost leaf can absorb the entire join, non-leftmost leaves must
-- fall back to plain `storage->read`.

DROP TABLE IF EXISTS t_left_empty;
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left_empty (id UInt64, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_left       (id UInt64, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_right      (id UInt64, val String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_left  VALUES (2, 'L2'), (3, 'L3'), (7, 'L7');
INSERT INTO t_right VALUES (2, 'R2'), (3, 'R3'), (5, 'R5'), (6, 'R6'), (7, 'R7');

SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;

-- Empty left with default join-order algo (the original repro from issue #89166).
SELECT '--- empty left, default algo ---';
SELECT r.id, r.val
FROM t_left_empty AS l INNER JOIN t_right AS r ON l.id = r.id
ORDER BY r.id;

-- Same query forcing the dpsize join-order algorithm.
SELECT '--- empty left, dpsize algo ---';
SELECT r.id, r.val
FROM t_left_empty AS l INNER JOIN t_right AS r ON l.id = r.id
ORDER BY r.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize';

-- Same query against the legacy (pre-new-logical-join) planner.
SELECT '--- empty left, new logical join step off ---';
SELECT r.id, r.val
FROM t_left_empty AS l INNER JOIN t_right AS r ON l.id = r.id
ORDER BY r.id
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Sanity check: non-empty left still absorbs the whole join via parallel replicas
-- (results must be correct).
SELECT '--- non-empty left, correctness ---';
SELECT l.id, r.val
FROM t_left AS l INNER JOIN t_right AS r ON l.id = r.id
ORDER BY l.id;

DROP TABLE t_left_empty;
DROP TABLE t_left;
DROP TABLE t_right;
