-- A join the optimizer has already settled enters its parent's join graph as a single relation, and
-- the row count it arrived at is the only estimate that relation has. The parent has to read it:
-- the cost model treats a missing count as one row, which makes the largest input in the query look
-- like the cheapest thing to join.
-- `query_plan_optimize_join_order_limit = 2` splits the three-table join deterministically - it is
-- optimized two relations at a time, so the first join re-enters the second graph as an input.
-- 100 distinct keys keep the `uniq_v2` counts exact, so the printed estimates are stable.

DROP TABLE IF EXISTS t_sub_left;
DROP TABLE IF EXISTS t_sub_right;
DROP TABLE IF EXISTS t_sub_top;

CREATE TABLE t_sub_left (k UInt32) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_sub_right (k UInt32) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_sub_top (k UInt32) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_sub_left SELECT number FROM numbers(100);
INSERT INTO t_sub_right SELECT number FROM numbers(100);
INSERT INTO t_sub_top SELECT number FROM numbers(10);

SET query_plan_optimize_join_order_randomize = 0; -- the test asserts on the join plan
SET query_plan_optimize_join_order_limit = 2; -- split the three-table join into two graphs
SET use_statistics = 1;
SET explain_query_plan_default = 'pretty'; -- the asserted lines are part of this format

-- The top join's left input is the already-optimized `t_sub_left`/`t_sub_right` join, so its
-- `Left: rows estimated` is the count handed across the subplan boundary; without propagation the
-- parent has nothing for that relation and prints `no stats`. The relation chain names the sub-join
-- and already carries its inputs' estimates, so it must not be given a second estimate of its own.
SELECT trimLeft(explain) AS plan
FROM (
    EXPLAIN SELECT count()
    FROM t_sub_left AS l
    INNER JOIN t_sub_right AS r ON l.k = r.k
    INNER JOIN t_sub_top AS t ON l.k = t.k
)
WHERE explain LIKE '%⋈%' OR explain LIKE '%rows estimated%' OR explain LIKE '%Output rows:%';

DROP TABLE t_sub_left;
DROP TABLE t_sub_right;
DROP TABLE t_sub_top;
