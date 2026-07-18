-- A stateful function (e.g. `neighbor`) depends on the order and blocking of the rows it observes.
-- A projection may have a different sort key and granularity than the base table, so replacing the
-- base-table read with a projection read (or projection index) would change the observed row/block
-- stream. Projection selection must therefore leave stateful queries on the base-table read.
--
-- The base table below is ordered by `k` (physical order of `v` is [2, 0, 1]); the projection is
-- ordered by `v` (physical order of `v` is [0, 1, 2]). `neighbor(v, 1) = v + 1` counts 1 on the
-- base read but 2 through the projection, so substituting the projection would produce a wrong result.

SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS t_proj_stateful;

CREATE TABLE t_proj_stateful (k UInt64, v UInt64, PROJECTION p_by_v (SELECT k, v ORDER BY v))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;

INSERT INTO t_proj_stateful VALUES (0, 2), (1, 0), (2, 1);

-- Control: for a non-stateful predicate the projection is usable, so forcing it succeeds.
SELECT 'control', count() FROM t_proj_stateful WHERE v > 0
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 1;
SELECT 'control', count() FROM t_proj_stateful WHERE v > 0
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 0;

-- A stateful function in the WHERE (FilterStep) forbids projection substitution, so forcing a
-- projection now errors instead of silently changing the observed rows.
SELECT count() FROM t_proj_stateful WHERE neighbor(v, 1) = v + 1
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 1; -- { serverError PROJECTION_NOT_USED }
SELECT count() FROM t_proj_stateful WHERE neighbor(v, 1) = v + 1
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 0; -- { serverError PROJECTION_NOT_USED }

-- The same for a stateful function in an explicit PREWHERE (reader-side filter on the base read).
SELECT count() FROM t_proj_stateful PREWHERE neighbor(v, 1) = v + 1
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 1; -- { serverError PROJECTION_NOT_USED }
SELECT count() FROM t_proj_stateful PREWHERE neighbor(v, 1) = v + 1
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 0; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_proj_stateful;

-- Aggregate projections are fenced through the same path.
DROP TABLE IF EXISTS t_agg_stateful;

CREATE TABLE t_agg_stateful (k UInt64, v UInt64, PROJECTION p_agg (SELECT v, count() GROUP BY v))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;

INSERT INTO t_agg_stateful VALUES (0, 2), (1, 0), (2, 1);

-- Control: the aggregate projection is usable for a plain aggregation.
SELECT 'agg-control', count() FROM t_agg_stateful GROUP BY v ORDER BY v
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 1;

-- A stateful predicate blocks the aggregate projection too.
SELECT count() FROM t_agg_stateful WHERE neighbor(v, 1) = v + 1 GROUP BY v ORDER BY v
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 1; -- { serverError PROJECTION_NOT_USED }
SELECT count() FROM t_agg_stateful WHERE neighbor(v, 1) = v + 1 GROUP BY v ORDER BY v
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 0; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_agg_stateful;

-- A stateful row policy (a reader-side row-level filter) must also keep the query on the base read.
DROP TABLE IF EXISTS t_rp_stateful;
DROP ROW POLICY IF EXISTS pol_stateful ON t_rp_stateful;

CREATE TABLE t_rp_stateful (k UInt64, v UInt64, PROJECTION p_by_v (SELECT k, v ORDER BY v))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;

INSERT INTO t_rp_stateful VALUES (0, 2), (1, 0), (2, 1);

CREATE ROW POLICY pol_stateful ON t_rp_stateful USING neighbor(v, 1) = v + 1 TO ALL;

SELECT count() FROM t_rp_stateful
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 1; -- { serverError PROJECTION_NOT_USED }
SELECT count() FROM t_rp_stateful
    SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, aggregate_functions_null_for_empty = 0, enable_parallel_replicas = 0, enable_analyzer = 0; -- { serverError PROJECTION_NOT_USED }

DROP ROW POLICY pol_stateful ON t_rp_stateful;
DROP TABLE t_rp_stateful;
