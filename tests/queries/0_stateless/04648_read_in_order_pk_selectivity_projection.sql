-- Tags: no-random-settings, no-random-merge-tree-settings
-- The read-in-order PK-selectivity guard (`read_in_order_max_primary_key_ratio`) is deliberately
-- exempt when the read comes from a projection with more than one selected part (the single-part
-- case is covered by `04843_read_in_order_pk_selectivity_projection_single_part`):
-- `optimizeUseNormalProjection` picks a normal
-- projection precisely because its own sorting key satisfies the outer `ORDER BY`, so the only
-- alternative plan is to read the same projection parts unordered and sort globally — the sort the
-- projection was chosen to remove. Measurements backing this exemption are in the setting
-- description and next to the check in `requestReadingInOrder`.
--
-- The exemption is a single, load-bearing check: `optimizeUseNormalProjection` propagates
-- `index_analysis_had_filter` to the projection reading step (it is built by `readFromParts` with a
-- ready analysis result, so `applyFilters` never runs on it), so the guard does reach these plans and
-- only the explicit `readFromProjection()` check keeps read-in-order. Without it the cases below flip.

DROP TABLE IF EXISTS t_read_in_order_projection;

-- The table is sorted by `ts`; the projection is sorted by `path`, so an `ORDER BY path` query
-- can only be answered in order by the projection.
CREATE TABLE t_read_in_order_projection
(
    ts UInt64,
    path String,
    v UInt64,
    PROJECTION p_by_path
    (
        SELECT ts, path, v
        ORDER BY path
    )
)
ENGINE = MergeTree ORDER BY ts
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_projection;

INSERT INTO t_read_in_order_projection SELECT number, concat('dir', toString(number % 997), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_read_in_order_projection SELECT number, concat('dir', toString(number % 997), '/file.log'), number FROM numbers(25000, 25000);

SET max_threads = 4;
SET enable_parallel_replicas = 0;

-- The projection is used (its sorting key is `path`) and the read is in order.
SELECT 'projection_used';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
) WHERE explain LIKE '%p_by_path%';

-- A leading-wildcard `LIKE` gives the projection's own primary key no pruning at all, so the
-- selectivity ratio is 1.0 and the guard would fire on a plain (non-projection) read. Reading from
-- a projection with more than one part (two here) is exempt, so read-in-order is kept and no global sort is inserted: the pipeline has
-- no `PartialSortingTransform`. The strictest threshold (`0.0`, guard always on), the default-like
-- `0.5` and `1.0` (guard fully off) must all agree.
SELECT 'projection_exempt_from_guard';
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT count() FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0., optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT count() FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 1., optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control that the setup is not simply too small for the guard: reading the base table in its own
-- sort order (`ORDER BY ts`) with the same unprunable filter does hit the guard — the primary key
-- selects every granule, so read-in-order is rejected and a sort appears. The very same table, data
-- and threshold therefore fire the guard off the projection path and stay exempt on it.
SELECT 'base_table_guard_fires';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY ts
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- And with the guard switched off that base-table query keeps read-in-order (no sort), which is
-- what makes the case above a real guard hit rather than a plan that never had read-in-order.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY ts
    SETTINGS read_in_order_max_primary_key_ratio = 1., optimize_use_projections = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Correctness: the projection read returns every row, in order.
SELECT 'correctness';
SELECT count() FROM (
    SELECT ts, path FROM t_read_in_order_projection
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, optimize_use_projections = 1
);

DROP TABLE t_read_in_order_projection;
