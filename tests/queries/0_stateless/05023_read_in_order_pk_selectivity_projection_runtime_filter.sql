-- The normal-projection rewrite must retain runtime-filter descriptors from the replaced read.
-- The filter below matches ~74% of the projection granules (more than the 0.5 threshold), so the
-- PK-selectivity guard would fire on the single-part projection read (deliberately not exempt) and
-- insert a full sort - unless the retained runtime filter marks the read as prunable at data-read
-- time (`mayPruneRangesOnDataRead`), which keeps the in-order read.

DROP TABLE IF EXISTS projection_runtime_filter_probe SYNC;
DROP TABLE IF EXISTS projection_runtime_filter_build SYNC;

CREATE TABLE projection_runtime_filter_probe
(
    a UInt64,
    path String,
    PROJECTION by_path (SELECT a, path ORDER BY path)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1024;

CREATE TABLE projection_runtime_filter_build (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO projection_runtime_filter_probe SELECT number, concat('path-', toString(number), '.log') FROM numbers(300000);
INSERT INTO projection_runtime_filter_build SELECT number FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_blocks_to_skip_before_reenabling = 0;
SET enable_join_runtime_filters = 1;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
-- A spilling hash join does not preserve the left side order, so read-in-order is not
-- propagated through it; disable automatic spilling to keep the in-order plan under test.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- The filter on `path` makes the projection win index analysis below the JOIN, and the retained
-- runtime filter exempts the read from the guard: the projection read stays in order, no full sort.
SELECT 'guard_exempted_by_runtime_filter', count() FROM
(
    EXPLAIN PIPELINE SELECT probe.a, probe.path
    FROM projection_runtime_filter_probe AS probe
    INNER JOIN projection_runtime_filter_build AS build USING (a)
    WHERE probe.path LIKE 'path-1%' OR probe.path LIKE 'path-2%'
    ORDER BY probe.path
    SETTINGS enable_join_runtime_filters_index_analysis = 1,
        use_skip_indexes_on_data_read = 1,
        read_in_order_max_primary_key_ratio = 0.5,
        optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: without runtime-filter index analysis nothing prunes the read at data-read time, so the
-- guard fires on the same plan and inserts a full sort - proving the setup is guard-sensitive.
SELECT 'guard_fires_without_runtime_filter', count() > 0 FROM
(
    EXPLAIN PIPELINE SELECT probe.a, probe.path
    FROM projection_runtime_filter_probe AS probe
    INNER JOIN projection_runtime_filter_build AS build USING (a)
    WHERE probe.path LIKE 'path-1%' OR probe.path LIKE 'path-2%'
    ORDER BY probe.path
    SETTINGS enable_join_runtime_filters_index_analysis = 0,
        use_skip_indexes_on_data_read = 1,
        read_in_order_max_primary_key_ratio = 0.5,
        optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'result_rows', count() FROM
(
    SELECT probe.a, probe.path
    FROM projection_runtime_filter_probe AS probe
    INNER JOIN projection_runtime_filter_build AS build USING (a)
    WHERE probe.path LIKE 'path-1%' OR probe.path LIKE 'path-2%'
    ORDER BY probe.path
    SETTINGS enable_join_runtime_filters_index_analysis = 1,
        use_skip_indexes_on_data_read = 1,
        read_in_order_max_primary_key_ratio = 0.5,
        optimize_use_projections = 1
);

DROP TABLE projection_runtime_filter_probe SYNC;
DROP TABLE projection_runtime_filter_build SYNC;
