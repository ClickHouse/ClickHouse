-- A `SQL SECURITY DEFINER` / `NONE` view whose plan contains a join is an optimization barrier
-- (a join can hide rows, so the whole view subplan is sealed). `tryTopKThroughJoin` used to peel
-- the sealed converting expression between the invoker's `Sorting` and the view's own `Join`
-- without looking at the barrier flag, and grafted the invoker's `Sort + Limit` below the seal
-- onto the join's preserved input, re-running the optimization passes on that subtree, so the
-- invoker's `ORDER BY ... LIMIT` retuned the reading inside the view.

-- Pin everything the plan shape depends on: the pass itself, the join runtime filters off (they
-- add extra `Filter` steps), a stable join side, and no deferral to the read-in-order
-- through-join pass (so the positive control reliably grafts).
-- `query_plan_max_limit_for_top_k_optimization` is randomized by the test harness and gates the
-- pass on the `LIMIT` value, so it is pinned too: with the randomized value of 1 not even the
-- positive control grafts and the oracle stops discriminating.
SET query_plan_top_k_through_join = 1, query_plan_read_in_order_through_join = 0,
    query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0, make_distributed_plan = 0, max_threads = 1,
    query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS l04891;
DROP TABLE IF EXISTS r04891;
CREATE TABLE l04891 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO l04891 SELECT number, number FROM numbers(100);
CREATE TABLE r04891 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO r04891 SELECT number, number FROM numbers(100);

CREATE VIEW v04891_invoker SQL SECURITY INVOKER AS
    SELECT l04891.v AS v, r04891.w AS w FROM l04891 LEFT JOIN r04891 ON l04891.k = r04891.k;
CREATE VIEW v04891_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT l04891.v AS v, r04891.w AS w FROM l04891 LEFT JOIN r04891 ON l04891.k = r04891.k;

SET enable_analyzer = 1;

-- The `INVOKER` view stays fully optimizable: `Sort + Limit` is grafted below the join, onto its
-- preserved input. This is the positive control proving that the plan-shape oracle discriminates.
SELECT 'invoker gets Sort+Limit below the join:',
    max(if(explain LIKE '%Limit%', rn, 0)) > min(if(explain LIKE '%Join%', rn, 1000000))
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn
    FROM (EXPLAIN compact = 0 SELECT * FROM v04891_invoker ORDER BY v LIMIT 10)
);

-- The `DEFINER` view is a barrier: nothing driven by the invoker's `ORDER BY ... LIMIT` may be
-- installed below the seal, so no `Limit` appears below the view's own `Join`.
SELECT 'definer keeps the join input untouched:',
    max(if(explain LIKE '%Limit%', rn, 0)) > min(if(explain LIKE '%Join%', rn, 1000000))
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn
    FROM (EXPLAIN compact = 0 SELECT * FROM v04891_definer ORDER BY v LIMIT 10)
);

-- The barrier only drops the optimization, never the result.
SELECT 'definer results:', groupArray(v) = [0, 1, 2, 3, 4], groupArray(w) = [0, 1, 2, 3, 4]
FROM (SELECT v, w FROM v04891_definer ORDER BY v LIMIT 5);

-- The legacy analyzer wraps the view's sort column in `materialize`, which already fails the
-- pass's pure-pass-through check, so neither twin grafts there: non-discriminating, pinned only
-- to catch a future regression that would make it fire.
SET enable_analyzer = 0;

SELECT 'legacy analyzer, definer keeps the join input untouched:',
    max(if(explain LIKE '%Limit%', rn, 0)) > min(if(explain LIKE '%Join%', rn, 1000000))
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn
    FROM (EXPLAIN compact = 0 SELECT * FROM v04891_definer ORDER BY v LIMIT 10)
);

SET enable_analyzer = DEFAULT;

DROP VIEW v04891_invoker;
DROP VIEW v04891_definer;
DROP TABLE l04891;
DROP TABLE r04891;
