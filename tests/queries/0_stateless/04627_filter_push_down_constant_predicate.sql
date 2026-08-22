-- Tags: no-parallel-replicas

-- A constant-true predicate (`AND 1`) combined with a `LEFT JOIN`, `ORDER BY` and `LIMIT` used to
-- throw `NOT_FOUND_COLUMN_IN_BLOCK`: with filter push down, merge expressions, top-k-through-join and
-- lazy materialization all active, the merged outer filter was left as a dangling input pass-through
-- in the lazy half of the plan, referencing a column no input block provides. Regression from #104268,
-- fixed on master. Issue #111452.
--
-- All optimizations that must be enabled to reach the fixed code path are pinned so the runner cannot
-- silently disable the path (which would let this test pass even on a broken build). The `*_max_limit_*`
-- caps use 0 = unlimited so they never drop below the query's `LIMIT`. `no-parallel-replicas` (like the
-- other lazy-materialization tests): parallel replicas rewrite the read before lazy materialization runs.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_merge_expressions = 1;
-- merged the pushed outer `cluster` predicate with the inner `ts` predicate into one filter node;
-- the `merged_filter_below_join` oracle column asserts that single merged node, so it must stay on.
SET query_plan_merge_filters = 1;
SET query_plan_top_k_through_join = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 0;
SET query_plan_max_limit_for_top_k_optimization = 0;
-- Pin the join orientation: under `auto` the logical join-order optimizer can swap this `ANY`
-- join's sides and reverse `LEFT` to `RIGHT`. The oracle below matches `Join` regardless of
-- orientation, so an unpinned swap would let it stay green on the wrong path.
SET query_plan_join_swap_table = false;

DROP TABLE IF EXISTS traces_04627;

CREATE TABLE traces_04627 (cluster String, ts UInt64, trace_id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO traces_04627 VALUES ('cluster1', 1, 'trace007');

-- Liveness oracle: assert the plan is the JOIN-specific top-k-through-join + lazy-materialization
-- path AND that the merged outer predicate was pushed below the join (the actual #111452 site), not
-- merely that some lazy stitch happened. `JoinLazyColumnsStep` alone is generic (single-table lazy
-- plans build it too, e.g. 02354_vector_search_queries), so it can stay green after the join rewrite
-- regresses. Five columns, all must be 1:
--   join_in_plan             - the SQL Join step is present.
--   preserved_side_topk_sort - top-k-through-join pushed a `Sorting` onto the preserved (left) input,
--                              i.e. a `Sorting` appears BELOW the Join in the DFS-printed plan.
--   lazy_stitch              - lazy materialization split the read (`JoinLazyColumnsStep`).
--   merged_filter_below_join - filter push down + merge filters left a single `Filter column:`
--                              carrying BOTH the outer `cluster` and inner `ts` predicates BELOW the
--                              Join. This is the merged dangling filter that #111452 mishandled; the
--                              other three columns survive independently of it.
--   merged_filter_keeps_const_conjunct - that same merged filter still carries the constant-true
--                              `AND 1` conjunct. #111452 fires only on the `AND 1` variant; if a
--                              planner cleanup folded `cluster = 'cluster1' AND 1` to just
--                              `cluster = 'cluster1'`, `merged_filter_below_join` would stay 1 while
--                              the trigger vanished, so this column pins the literal `1` conjunct.
--                              The match is form-agnostic (` AND 1` trailing, or ` 1 AND ` if
--                              reordered) because the predicate prints in function form
--                              (`equals(...) AND 1`) or operator form (`cluster = '...' AND 1`)
--                              depending on the randomized `query_plan_optimize_prewhere`.
WITH plan AS
(
    SELECT rowNumberInAllBlocks() AS rn, explain
    FROM
    (
        EXPLAIN actions = 1
        SELECT ts
        FROM
        (
            SELECT t.cluster, t.ts
            FROM traces_04627 AS t
            LEFT ANY JOIN (SELECT '' AS cluster, '' AS trace_id) AS a USING (cluster, trace_id)
            WHERE t.ts BETWEEN 0 AND 10
        ) AS v
        WHERE cluster = 'cluster1' AND 1
        ORDER BY ts DESC
        LIMIT 10
    )
)
SELECT
    (SELECT count() FROM plan WHERE explain ILIKE '%Join (JOIN%') > 0 AS join_in_plan,
    (SELECT count() FROM plan WHERE explain ILIKE '%Sorting%'
             AND rn > (SELECT min(rn) FROM plan WHERE explain ILIKE '%Join (JOIN%')) > 0 AS preserved_side_topk_sort,
    (SELECT count() FROM plan WHERE explain ILIKE '%JoinLazyColumnsStep%') > 0 AS lazy_stitch,
    (SELECT count() FROM plan WHERE explain ILIKE '%ilter column:%'
             AND explain ILIKE '%cluster%' AND explain ILIKE '%ts%'
             AND rn > (SELECT min(rn) FROM plan WHERE explain ILIKE '%Join (JOIN%')) > 0 AS merged_filter_below_join,
    (SELECT count() FROM plan WHERE explain ILIKE '%ilter column:%'
             AND explain ILIKE '%cluster%' AND explain ILIKE '%ts%'
             AND match(explain, '( AND 1$)|( 1 AND )')
             AND rn > (SELECT min(rn) FROM plan WHERE explain ILIKE '%Join (JOIN%')) > 0 AS merged_filter_keeps_const_conjunct;

-- The regression itself: must return a single row, ts = 1.
SELECT ts
FROM
(
    SELECT t.cluster, t.ts
    FROM traces_04627 AS t
    LEFT ANY JOIN (SELECT '' AS cluster, '' AS trace_id) AS a USING (cluster, trace_id)
    WHERE t.ts BETWEEN 0 AND 10
) AS v
WHERE cluster = 'cluster1' AND 1
ORDER BY ts DESC
LIMIT 10;

DROP TABLE traces_04627;
