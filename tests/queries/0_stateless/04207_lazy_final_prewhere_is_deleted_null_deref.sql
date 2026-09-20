-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for a UBSan/null pointer dereference in the lazy FINAL
-- optimization (`addIsDeletedFilter` -> `ActionsDAG::addFunction`).
--
-- When a query specifies `PREWHERE` with a compound expression that references
-- the `is_deleted` column (e.g. `PREWHERE is_deleted = 0 AND category = 'X'`),
-- the prewhere actions DAG consumes `is_deleted` as an input but does not expose
-- it as an output. After lazy FINAL builds a non-FINAL reading step
-- (`setHasFinal(false)`), `removeUnusedColumns` no longer treats `is_deleted` as
-- required for merging, so the column is erased from the read step's output by
-- `ActionsDAG::updateHeader`. The downstream `addIsDeletedFilter` step then
-- iterated the header looking for `is_deleted`, did not find it, and passed
-- a null `Node *` to `ActionsDAG::addFunction`, producing UBSan
-- `reference binding to null pointer of type 'const DB::ActionsDAG::Node'`
-- in release builds and a segfault in debug.
--
-- Reported via server-side AST fuzzer (STID 1720-3759).

DROP TABLE IF EXISTS t_lazy_final_prewhere_del;

CREATE TABLE t_lazy_final_prewhere_del
(
    key UInt64,
    version UInt64,
    is_deleted UInt8,
    category String,
    value UInt64
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY key
SETTINGS index_granularity = 64;

SYSTEM STOP MERGES t_lazy_final_prewhere_del;

-- Two intersecting parts (overlap on 200..499)
INSERT INTO t_lazy_final_prewhere_del
SELECT number, 1, 0, if(number < 50, 'target', 'other'), number
FROM numbers(500);

INSERT INTO t_lazy_final_prewhere_del
SELECT number + 200, 2, if(number < 50, 1, 0), if(number < 150, 'target', 'other'), (number + 200) * 10
FROM numbers(500);

-- Two non-intersecting parts (so that `trySplitNonIntersectingParts` actually splits)
INSERT INTO t_lazy_final_prewhere_del
SELECT number + 1000, 1, if(number >= 400, 1, 0), if(number < 200, 'target', 'other'), (number + 1000) * 100
FROM numbers(500);

INSERT INTO t_lazy_final_prewhere_del
SELECT number + 2000, 1, 0, if(number < 50, 'target', 'other'), (number + 2000) * 100
FROM numbers(500);

-- The trigger: PREWHERE references `is_deleted` together with another column,
-- so the prewhere DAG's outputs drop `is_deleted`. Before the fix this aborts
-- the server.
SELECT count()
FROM t_lazy_final_prewhere_del FINAL
PREWHERE is_deleted = 0 AND category = 'target'
SETTINGS query_plan_optimize_lazy_final = 1,
         max_rows_for_lazy_final = 10000000,
         min_filtered_ratio_for_lazy_final = 0;

-- Same with `EXPLAIN` (the optimization runs for `EXPLAIN` too — before the fix
-- this also aborted the server). We only assert that we get a non-empty plan.
SELECT count() > 0
FROM (
    EXPLAIN actions = 0
    SELECT count() FROM t_lazy_final_prewhere_del FINAL
    PREWHERE is_deleted = 0 AND category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
);

-- Mixing PREWHERE on `is_deleted` with WHERE on another column, to cover the
-- code path where the WHERE clause becomes the parent `FilterStep` while
-- prewhere has already consumed `is_deleted`.
SELECT count()
FROM t_lazy_final_prewhere_del FINAL
PREWHERE is_deleted = 0
WHERE category = 'target'
SETTINGS query_plan_optimize_lazy_final = 1,
         max_rows_for_lazy_final = 10000000,
         min_filtered_ratio_for_lazy_final = 0;

-- Same query, optimization OFF — sanity check the result is consistent
-- with the optimized branch above.
SELECT count()
FROM t_lazy_final_prewhere_del FINAL
PREWHERE is_deleted = 0 AND category = 'target'
SETTINGS query_plan_optimize_lazy_final = 0;

DROP TABLE t_lazy_final_prewhere_del;
