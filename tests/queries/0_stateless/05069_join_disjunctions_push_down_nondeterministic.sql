-- https://github.com/ClickHouse/ClickHouse/issues/116930
-- `use_join_disjunctions_push_down` extracts a per-side partial predicate and pushes it below the
-- join while keeping the original filter on top. A non-deterministic conjunct is then drawn twice
-- per row, independently, so a row that satisfies the query's own filter can still be discarded by
-- the pre-filter's draw. The main filter pushdown refuses to move such conjuncts for the same reason.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_disj_left;
DROP TABLE IF EXISTS t_disj_right;
CREATE TABLE t_disj_left (a UInt32, k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_disj_right (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_disj_left SELECT 1, number FROM numbers(1000000);
INSERT INTO t_disj_right SELECT number FROM numbers(1000000);

-- One draw per row selects about half of the million rows.
SELECT count() BETWEEN 490000 AND 510000
FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND rand() % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 1;

SELECT count() BETWEEN 490000 AND 510000
FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND rand() % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 0;

-- A deterministic predicate is still pushed.
SELECT 'deterministic';
SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND t_disj_left.k % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 1;
SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND t_disj_left.k % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 0;

DROP TABLE t_disj_left;
DROP TABLE t_disj_right;
