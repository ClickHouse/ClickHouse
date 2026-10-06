-- https://github.com/ClickHouse/ClickHouse/issues/116930
-- `use_join_disjunctions_push_down` extracts a per-side partial predicate and pushes it below the
-- join while keeping the original filter on top. A non-deterministic conjunct is then drawn twice
-- per row, independently, so a row that satisfies the query's own filter can still be discarded by
-- the pre-filter's draw. The main filter pushdown refuses to move such conjuncts for the same reason.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
-- With parallel replicas the plan is printed for the initiator and for the replica, so every
-- pre-filter below would be listed twice.
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
-- The plan assertions below sort the matched lines: the order in which the plan prints the filter
-- above the join, the pre-filters below it and the `PREWHERE` steps depends on which side the join
-- is read from, which the settings randomizer is free to flip.

DROP TABLE IF EXISTS t_disj_left;
DROP TABLE IF EXISTS t_disj_right;
CREATE TABLE t_disj_left (a UInt32, k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_disj_right (k UInt32, x UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_disj_left SELECT 1, number FROM numbers(1000000);
INSERT INTO t_disj_right SELECT number, 100 FROM numbers(1000000);

-- One draw per row selects about half of the million rows. Before the fix the conjunct was cloned
-- into the pre-filter below the join and drawn a second time, so about a quarter of the rows
-- survived both draws.
SELECT count() BETWEEN 490000 AND 510000
FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND rand() % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 1;

SELECT count() BETWEEN 490000 AND 510000
FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND rand() % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 0;

-- A predicate whose branches touch both sides of the join is what the optimization is for: with it
-- enabled the plan gains one pre-filter per side below the join, with it disabled only the original
-- filter above the join remains. Asserting the plan and not just the result is what makes the block
-- below notice a regression that stops extracting deterministic partial predicates altogether.
SELECT 'deterministic plan (enabled)';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
    WHERE (t_disj_left.a = 1 AND t_disj_right.x = 100) OR (t_disj_left.a = 2 AND t_disj_right.x = 200)
    SETTINGS use_join_disjunctions_push_down = 1
) WHERE explain ILIKE '%Filter column:%' ORDER BY 1 FORMAT TSV;

SELECT 'deterministic plan (disabled)';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
    WHERE (t_disj_left.a = 1 AND t_disj_right.x = 100) OR (t_disj_left.a = 2 AND t_disj_right.x = 200)
    SETTINGS use_join_disjunctions_push_down = 0
) WHERE explain ILIKE '%Filter column:%' ORDER BY 1 FORMAT TSV;

-- The same predicate with a non-deterministic conjunct added: the deterministic parts are still
-- extracted, but neither pre-filter mentions `rand`.
SELECT 'nondeterministic plan';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
    WHERE (t_disj_left.a = 1 AND t_disj_right.x = 100 AND rand() % 2 = 0) OR (t_disj_left.a = 2 AND t_disj_right.x = 200)
    SETTINGS use_join_disjunctions_push_down = 1
) WHERE explain ILIKE '%Filter column:%' ORDER BY 1 FORMAT TSV;

-- A stateful predicate must not be cloned below the join either: `tryPushDownFilter` returns early
-- for a filter whose expression `hasStatefulFunctions`, so nothing at all is extracted and the plan
-- has no pre-filter below the join.
SELECT 'stateful plan';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
    WHERE (t_disj_left.a = 1 AND t_disj_right.x = 100 AND timeSeriesTagsToGroup([('x', 'y')]) = 0) OR (t_disj_left.a = 2 AND t_disj_right.x = 200)
    SETTINGS use_join_disjunctions_push_down = 1
) WHERE explain ILIKE '%Filter column:%' ORDER BY 1 FORMAT TSV;

-- The extracted deterministic predicate does not change the result.
SELECT 'deterministic';
SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND t_disj_left.k % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 1;
SELECT count() FROM t_disj_left JOIN t_disj_right ON t_disj_left.k = t_disj_right.k
WHERE (t_disj_left.a = 1 AND t_disj_left.k % 2 = 0) OR (t_disj_left.a = 2)
SETTINGS use_join_disjunctions_push_down = 0;

DROP TABLE t_disj_left;
DROP TABLE t_disj_right;
