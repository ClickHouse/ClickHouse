SET enable_analyzer = 1;

-- Regression test for block structure mismatch in `removeUnusedColumns` with `FilterStep` and FINAL.
--
-- When a query has a WHERE clause (not PREWHERE), the query plan contains a `FilterStep`.
-- `tryRemoveUnusedColumns` asks `FilterStep` to remove columns its parent does not need.
-- `FilterStep` removes the sort-key and version columns from its input header (they are not
-- needed by the parent's SELECT), returning `OutputAndInput`, which triggers
-- `removeChildrenOutputs(FilterStep)`.
--
-- Inside `removeChildrenOutputs`, `ReadFromMergeTree` with FINAL returns `None` from
-- `removeUnusedColumns` because it must keep sort-key and version columns for merging —
-- they were already in its output and nothing changed. This means the
-- `addDiscardingExpressionStepIfNeeded` path is skipped (it only runs when `updatedAnything`).
--
-- The result is a mismatch: `FilterStep` input header says {val} but its child
-- `ReadFromMergeTree` outputs {key, val, ver}. The previous code called
-- `absorbExtraChildColumns`, which only handled `ExpressionStep` and returned false for
-- `FilterStep`, leaving the mismatch unresolved and triggering the
-- "Block structure mismatch in after removing unused columns" assertion in debug / sanitizer builds.
--
-- The fix: when `absorbExtraChildColumns` returns false, fall back to inserting a discarding
-- `ExpressionStep` between the `FilterStep` and `ReadFromMergeTree` to bridge the gap.

DROP TABLE IF EXISTS t_where_final_ruc;

CREATE TABLE t_where_final_ruc
(
    key   Int64,
    val   Int64,
    ver   UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key;

INSERT INTO t_where_final_ruc VALUES (1, 10, 1);
INSERT INTO t_where_final_ruc VALUES (1, 20, 2);
INSERT INTO t_where_final_ruc VALUES (2,  5, 1);
INSERT INTO t_where_final_ruc VALUES (3, 15, 1);

-- FilterStep (WHERE) + FINAL: key and ver are not in SELECT and not in WHERE, so
-- FilterStep will try to remove them from its input. ReadFromMergeTree FINAL must keep
-- them for merging. This combination previously caused a block-structure-mismatch assertion.
SELECT val FROM t_where_final_ruc FINAL WHERE val > 5 ORDER BY val;

-- CollapsingMergeTree: sign column is required for FINAL but not in SELECT or WHERE.
DROP TABLE IF EXISTS t_where_final_ruc_col;

CREATE TABLE t_where_final_ruc_col
(
    key  Int64,
    val  Int64,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY key;

INSERT INTO t_where_final_ruc_col VALUES (1, 10, 1);
INSERT INTO t_where_final_ruc_col VALUES (2,  5, 1);
INSERT INTO t_where_final_ruc_col VALUES (3, 15, 1);

SELECT val FROM t_where_final_ruc_col FINAL WHERE val > 5 ORDER BY val;

DROP TABLE t_where_final_ruc;
DROP TABLE t_where_final_ruc_col;
