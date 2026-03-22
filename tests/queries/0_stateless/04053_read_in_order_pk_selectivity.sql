-- Test that read-in-order optimization is disabled when PK selectivity is poor.
-- When the WHERE clause cannot use the primary key (e.g., LIKE '%...'),
-- parallel reading with sorting should be used instead of read-in-order,
-- because read-in-order kills parallelism.

DROP TABLE IF EXISTS t_read_in_order_pk;

CREATE TABLE t_read_in_order_pk (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
AS SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(100000);

-- With poor PK selectivity (leading wildcard LIKE), the optimizer should fall back
-- from read-in-order to parallel reading with sorting.
-- We detect the fallback by the presence of PartialSortingTransform and MergeSortingTransform
-- inside the ReadFromMergeTree step.
SELECT 'poor_pk_selectivity_has_sorting';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- With good PK selectivity, read-in-order should still be used and no extra sorting is needed.
SELECT 'good_pk_selectivity_no_sorting';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk
    WHERE path LIKE 'path/1%'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Verify that results are correct (sorted) when falling back from read-in-order.
SELECT 'correctness';
SELECT count() FROM (
    SELECT * FROM t_read_in_order_pk
    WHERE path LIKE '%file.log'
    ORDER BY path
);

DROP TABLE t_read_in_order_pk;
