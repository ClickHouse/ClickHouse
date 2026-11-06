-- Tags: no-old-analyzer

-- The query plan optimizer passes that key on a physical `JoinStep` and its `TableJoin` do not
-- recognize `BlockNestedLoopJoinStep`. This pins that each of them degrades by skipping the step
-- rather than by concluding that the plan holds no join: a `WHERE` above the operator is pushed
-- down while the join is still logical, a condition that spans both sides stays above it, a
-- sorting the operator does not need is still removed, and every rewrite leaves the result alone.

SET join_algorithm = 'direct,parallel_hash,hash';
SET cross_to_inner_join_rewrite = 0;
SET query_plan_join_swap_table = 'false';
-- Pinned because the plan assertions below name where the filter landed, and the test harness
-- randomizes all four.
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS bnl_l;
DROP TABLE IF EXISTS bnl_r;

CREATE TABLE bnl_l (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bnl_r (id Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO bnl_l SELECT number + 1, number % 5 FROM numbers(10);
INSERT INTO bnl_r SELECT number + 1, number % 4 FROM numbers(8);

-- A single-side `WHERE` reaches the input it names, down to the `PREWHERE` of the reading step,
-- and leaves nothing above the join.
SELECT 'left routed', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'left pushed', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%Prewhere filter column:  id > 3%';
SELECT 'left nothing above', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%Filter (WHERE)%';

SELECT 'right routed', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y WHERE r.id > 3)
WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'right pushed', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y WHERE r.id > 3)
WHERE explain LIKE '%Prewhere filter column:  id > 3%';
SELECT 'right nothing above', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y WHERE r.id > 3)
WHERE explain LIKE '%Filter (WHERE)%';

SELECT 'semi routed', count() FROM (EXPLAIN SELECT l.id FROM bnl_l l LEFT SEMI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'semi pushed', count() FROM (EXPLAIN SELECT l.id FROM bnl_l l LEFT SEMI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%Prewhere filter column:  id > 3%';

SELECT 'anti routed', count() FROM (EXPLAIN SELECT l.id FROM bnl_l l LEFT ANTI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'anti pushed', count() FROM (EXPLAIN SELECT l.id FROM bnl_l l LEFT ANTI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%Prewhere filter column:  id > 3%';

-- A `WHERE` on the padded side of an outer join drops the padded rows, so the kind narrows first:
-- `FULL` becomes `LEFT` and keeps the operator, `LEFT` becomes `INNER` and goes back to the cross
-- join with a filter that `INNER` with `ALL` strictness uses today.
SELECT 'full narrowed to left', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l FULL JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
WHERE explain LIKE '%Type: LEFT%';
SELECT 'left narrowed to inner', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE r.id > 3)
WHERE explain LIKE '%Type: cross%';

-- A condition over both sides cannot be pushed anywhere and stays above the operator.
SELECT 'both sides routed', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.x + r.y > 4)
WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'both sides above', count() FROM (EXPLAIN SELECT l.id, r.id FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.x + r.y > 4)
WHERE explain LIKE '%Filter (WHERE)%';

-- Whichever way the filter goes, the result is the one the unoptimized plan produces.
SELECT 'left same', (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
  = (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.id > 3
     SETTINGS query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0) AS ok;
SELECT 'right same', (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y WHERE r.id > 3)
  = (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y WHERE r.id > 3
     SETTINGS query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0) AS ok;
SELECT 'full same', (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l FULL JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
  = (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l FULL JOIN bnl_r r ON l.x < r.y WHERE l.id > 3
     SETTINGS query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0, query_plan_convert_outer_join_to_inner_join = 0) AS ok;
SELECT 'semi same', (SELECT arraySort(groupArray(l.id)) FROM bnl_l l LEFT SEMI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
  = (SELECT arraySort(groupArray(l.id)) FROM bnl_l l LEFT SEMI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3
     SETTINGS query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0) AS ok;
SELECT 'anti same', (SELECT arraySort(groupArray(l.id)) FROM bnl_l l LEFT ANTI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3)
  = (SELECT arraySort(groupArray(l.id)) FROM bnl_l l LEFT ANTI JOIN bnl_r r ON l.x < r.y WHERE l.id > 3
     SETTINGS query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0) AS ok;
SELECT 'both sides same', (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.x + r.y > 4)
  = (SELECT arraySort(groupArray((l.id, r.id))) FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y WHERE l.x + r.y > 4
     SETTINGS query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0) AS ok;

DROP TABLE bnl_l;
DROP TABLE bnl_r;
