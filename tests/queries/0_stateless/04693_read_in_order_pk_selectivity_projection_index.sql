-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression for the read-in-order PK-selectivity guard misfiring when a projection is used as an
-- index and its mark ranges are refined inside the read pools (`use_projection_index_in_read_pools`).
--
-- `filterPartsAndCollectProjectionCandidates` registers the projection read ranges on the reading step
-- during `optimizeUseNormalProjections`, which runs before `optimizeReadInOrder`. Index analysis applies
-- only the *part*-level effect of that projection, so for a surviving part `selected_marks` still counts
-- every mark: the ranges are refined later, at read time, by the `ProjectionIndexReadRangesRefiner` that
-- the in-order read pools install. Judged by that upper bound a read that ends up touching two granules
-- looked like a full scan, and the guard replaced a cheap in-order streaming read with a global sort.

DROP TABLE IF EXISTS t_read_in_order_projection_index;

CREATE TABLE t_read_in_order_projection_index
(
    id UInt64,
    region String,
    value UInt64,
    PROJECTION region_proj INDEX region TYPE basic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

-- All 'rare' rows live in two marks out of 256, so the projection index prunes almost the whole read,
-- while the primary key (`id`) cannot use `WHERE region = 'rare'` at all.
INSERT INTO t_read_in_order_projection_index
SELECT number, if(number BETWEEN 1600 AND 1610 OR number BETWEEN 1700 AND 1710, 'rare', 'common'), number * 10
FROM numbers(4096);

OPTIMIZE TABLE t_read_in_order_projection_index FINAL;

SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;

-- The guard must not fire: the mark count it can see is a pre-refinement upper bound.
SELECT 'projection_index_pruning_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_projection_index
    WHERE region = 'rare'
    ORDER BY id
    SETTINGS use_projection_index_in_read_pools = 1, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: with the projection index not used in the read pools no refiner is installed, the read really
-- is the whole table and the guard must still fire. This also shows that the exemption is what keeps
-- read-in-order in the case above, rather than the table being too small for the guard to consider.
SELECT 'no_read_pool_refinement_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_projection_index
    WHERE region = 'rare'
    ORDER BY id
    SETTINGS use_projection_index_in_read_pools = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: a predicate no projection index covers leaves nothing to refine even with the setting on,
-- so the guard must fire there too.
SELECT 'poor_selectivity_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_projection_index
    WHERE value % 7 = 3
    ORDER BY id
    SETTINGS use_projection_index_in_read_pools = 1, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Correctness: the exempted read-in-order query returns all matching rows, sorted.
SELECT 'correctness';
SELECT count(), min(id), max(id) FROM (
    SELECT id FROM t_read_in_order_projection_index
    WHERE region = 'rare'
    ORDER BY id
    SETTINGS use_projection_index_in_read_pools = 1, read_in_order_max_primary_key_ratio = 0.5
);

DROP TABLE t_read_in_order_projection_index;
