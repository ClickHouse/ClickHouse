-- The normal-projection rewrite must retain runtime-filter descriptors from the replaced read.
-- The projection is one part, so it is deliberately not exempt from the PK-selectivity guard.
-- The runtime filter prunes the data during reading; therefore the guard must not insert a full sort.

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
SET join_runtime_filter_min_probe_rows = 1;
SET join_runtime_filter_blocks_to_skip_before_reenabling = 0;
SET enable_join_runtime_filters = 1;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT probe.a, probe.path
    FROM projection_runtime_filter_probe AS probe
    INNER JOIN projection_runtime_filter_build AS build USING (a)
    WHERE path LIKE '%log'
    ORDER BY path
    SETTINGS enable_join_runtime_filters_index_analysis = 1,
        use_skip_indexes_on_data_read = 1,
        read_in_order_max_primary_key_ratio = 0.5,
        optimize_use_projections = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT count() FROM
(
    SELECT probe.a, probe.path
    FROM projection_runtime_filter_probe AS probe
    INNER JOIN projection_runtime_filter_build AS build USING (a)
    WHERE path LIKE '%log'
    ORDER BY path
    SETTINGS enable_join_runtime_filters_index_analysis = 1,
        use_skip_indexes_on_data_read = 1,
        read_in_order_max_primary_key_ratio = 0.5,
        optimize_use_projections = 1
);

DROP TABLE projection_runtime_filter_probe SYNC;
DROP TABLE projection_runtime_filter_build SYNC;
