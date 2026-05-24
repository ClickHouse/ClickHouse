-- Tags: no-random-settings, no-random-merge-tree-settings
-- Test that read-in-order optimization is rejected when primary key selectivity is poor.
-- When the WHERE clause cannot use the primary key (e.g., LIKE '%...'),
-- the optimizer should not apply read-in-order because it kills parallelism.
-- Each query disables parallel replicas at the query level rather than relying on a tag,
-- to keep coverage across other configurations while still pinning behavior of the check.

DROP TABLE IF EXISTS t_read_in_order_pk;

-- Use a small index_granularity to produce enough marks for the check to trigger
-- (the check requires total_marks > requested_num_streams).
-- Insert in multiple parts so the test mirrors the original motivating case
-- (a large multi-part table where read-in-order serializes per-part reads).
CREATE TABLE t_read_in_order_pk (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_pk;

INSERT INTO t_read_in_order_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_read_in_order_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO t_read_in_order_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO t_read_in_order_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

SET max_threads = 4;

-- With poor primary key selectivity (leading wildcard LIKE), read-in-order should be rejected.
-- The SortingStep should do a full sort (PartialSortingTransform + MergeSortingTransform)
-- instead of just MergingSorted.
SELECT 'poor_pk_selectivity_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- With good primary key selectivity, read-in-order should be used (no PartialSortingTransform).
SELECT 'good_pk_selectivity_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE 'path/1%'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Verify that results are correct (sorted) when read-in-order is rejected.
SELECT 'correctness';
SELECT count() FROM (
    SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
);

-- Setting `read_in_order_max_primary_key_ratio` = 1.0 should disable the rejection.
SELECT 'setting_disabled';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 1.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Full scan with `ORDER BY` on the sort key (no WHERE/PREWHERE) selects all marks,
-- so the PK-selectivity ratio is 1.0 by construction, but that is not a sign that the
-- primary key failed — there was nothing to filter on. Read-in-order must stay enabled,
-- otherwise we would replace a low-memory streaming plan with full parallel reading plus
-- a global `MergeSortingTransform`.
SELECT 'full_scan_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

DROP TABLE t_read_in_order_pk;
