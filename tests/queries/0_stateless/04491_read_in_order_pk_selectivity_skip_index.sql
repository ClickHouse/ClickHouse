-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression for the read-in-order PK-selectivity guard misfiring when a *secondary* (skip) index,
-- rather than the primary key, is what prunes the read.
--
-- The guard disables read-in-order (falling back to parallel reading + a global sort) when the
-- primary key fails to prune the granules to read. It measured that only via `selected_marks_pk`,
-- the mark count after the primary-key step. But `selected_marks_pk` ignores skip indexes: for a
-- table `ORDER BY id` with a selective skip index on a non-sorting-key column, a query like
-- `WHERE grp = ... ORDER BY id` has `selected_marks_pk == total_marks_pk` (the primary key cannot
-- use `grp`) even though the skip index reduced the *final* `selected_marks` to a small fraction.
-- Firing the guard there replaces a low-memory in-order streaming read of the already-pruned ranges
-- with a global sort, a performance/memory regression. The guard now also requires the final
-- `selected_marks` ratio to exceed the threshold, so it stays disabled here and read-in-order is kept.

DROP TABLE IF EXISTS t_read_in_order_skip_index;

-- Small index_granularity so the table has plenty of marks (the guard requires total_marks > streams).
-- `grp = intDiv(id, 10000)` is clustered along the sort order, so a minmax index on it prunes cleanly.
CREATE TABLE t_read_in_order_skip_index
(
    id UInt64,
    grp UInt64,
    s String,
    INDEX idx_grp grp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_skip_index;

-- Insert as several parts covering disjoint id ranges, so `grp` stays clustered inside each part
-- (mirrors the multi-part motivating case where read-in-order serializes per-part reads).
INSERT INTO t_read_in_order_skip_index SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(0, 25000);
INSERT INTO t_read_in_order_skip_index SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(25000, 25000);
INSERT INTO t_read_in_order_skip_index SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(50000, 25000);
INSERT INTO t_read_in_order_skip_index SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(75000, 25000);

SET max_threads = 4;

-- The primary key (`id`) cannot use `WHERE grp = 5`, so `selected_marks_pk == total_marks_pk` and the
-- old guard fired. But the minmax skip index on `grp` prunes the read to a small fraction of the marks,
-- so the final selection is small and read-in-order must be kept: no PartialSortingTransform.
SELECT 'skip_index_prunes_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_skip_index
    WHERE grp = 5
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, force_data_skipping_indices = 'idx_grp', read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: a leading-wildcard LIKE on a non-indexed column cannot be pruned by either the primary key
-- or the skip index, so the final selection is the whole table. Here the guard must still fire: it
-- disables read-in-order and does a full sort (PartialSortingTransform present). This proves the added
-- final-selection check did not simply switch the guard off.
SELECT 'poor_selectivity_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_skip_index
    WHERE s LIKE '%9'
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Correctness: the skip-index-pruned, read-in-order query still returns all matching rows, sorted.
SELECT 'correctness';
SELECT count() FROM (
    SELECT * FROM t_read_in_order_skip_index
    WHERE grp = 5
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
);

DROP TABLE t_read_in_order_skip_index;
