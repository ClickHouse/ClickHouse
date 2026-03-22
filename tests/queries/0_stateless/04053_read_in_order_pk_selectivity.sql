-- Test that read-in-order optimization is rejected when primary key selectivity is poor.
-- When the WHERE clause cannot use the primary key (e.g., LIKE '%...'),
-- the optimizer should not apply read-in-order because it kills parallelism.

DROP TABLE IF EXISTS t_read_in_order_pk;

CREATE TABLE t_read_in_order_pk (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
AS SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(100000);

-- With poor primary key selectivity (leading wildcard LIKE), read-in-order should be rejected.
-- The SortingStep should do a full sort (PartialSortingTransform + MergeSortingTransform)
-- instead of just MergingSorted.
SELECT 'poor_pk_selectivity_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- With good primary key selectivity, read-in-order should be used (no PartialSortingTransform).
SELECT 'good_pk_selectivity_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE 'path/1%'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Verify that results are correct (sorted) when read-in-order is rejected.
SELECT 'correctness';
SELECT count() FROM (
    SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
);

-- Setting `read_in_order_max_primary_key_ratio` = 1.0 should disable the rejection.
SELECT 'setting_disabled';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 1.0
) WHERE explain LIKE '%PartialSortingTransform%';

DROP TABLE t_read_in_order_pk;
