-- A row policy on a MaterializedView whose column type drifts from the target is applied as a
-- FilterStep merged with the structure-conversion cast, whose DAG computes `CAST(x, ...) AS x`
-- over an input also named `x`. Splitting such a DAG for lazy materialization renames the node
-- promoted to the lazy half's input, and the main branch then failed to resolve the original
-- name (NOT_FOUND_COLUMN_IN_BLOCK). The optimization must skip such plans.

DROP TABLE IF EXISTS lm_mv_src;
DROP TABLE IF EXISTS lm_mv_dst;
DROP VIEW IF EXISTS lm_mv;

CREATE TABLE lm_mv_src (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE lm_mv_dst (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW lm_mv TO lm_mv_dst AS SELECT CAST(x, 'Nullable(UInt64)') AS x, y FROM lm_mv_src;
INSERT INTO lm_mv_dst SELECT number, number + 1 FROM numbers(10);

CREATE ROW POLICY rp_04654 ON lm_mv FOR SELECT USING x != 0 TO CURRENT_USER;

SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;

SELECT x, y FROM lm_mv ORDER BY x LIMIT 3;
SELECT x, y FROM lm_mv ORDER BY x LIMIT 3 SETTINGS enable_analyzer = 0;

DROP ROW POLICY rp_04654 ON lm_mv;
DROP VIEW lm_mv;
DROP TABLE lm_mv_dst;
DROP TABLE lm_mv_src;
