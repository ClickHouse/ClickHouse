-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression for the read-in-order PK-selectivity guard misfiring when skip-index pruning is deferred
-- to the data read (`use_skip_indexes_on_data_read = 1`, the default).
--
-- The guard requires the final selection (`selected_marks / total_marks_pk`) to be large too, so that a
-- query whose primary key prunes nothing but whose skip index prunes the read keeps read-in-order (see
-- `04491_read_in_order_pk_selectivity_skip_index`). But `selected_marks` is a *pre*-pruning count when a
-- `MergeTreeSkipIndexReader` is installed: index analysis then skips the useful skip indexes and the reader
-- applies them per granule during the read. Judged by that upper bound, a read that ends up touching a few
-- granules looks like a full scan, and the guard replaced a cheap in-order streaming read with a global
-- sort. The guard is now exempt whenever range pruning may happen during the read.

DROP TABLE IF EXISTS t_read_in_order_data_read_pruning;

-- Small index_granularity so the table has plenty of marks (the guard requires total_marks > streams).
-- `grp = intDiv(id, 10000)` is clustered along the sort order, so a minmax index on it prunes cleanly.
CREATE TABLE t_read_in_order_data_read_pruning
(
    id UInt64,
    grp UInt64,
    s String,
    INDEX idx_grp grp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_data_read_pruning;

INSERT INTO t_read_in_order_data_read_pruning SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(0, 25000);
INSERT INTO t_read_in_order_data_read_pruning SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(25000, 25000);
INSERT INTO t_read_in_order_data_read_pruning SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(50000, 25000);
INSERT INTO t_read_in_order_data_read_pruning SELECT number, intDiv(number, 10000), concat('s', toString(number % 1000)) FROM numbers(75000, 25000);

SET max_threads = 4;

-- The primary key (`id`) cannot use `WHERE grp = 5`, and with pruning deferred to the data read the mark
-- counts the guard sees are the pre-pruning ones, so it must not fire: no `PartialSortingTransform`.
SELECT 'data_read_pruning_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_data_read_pruning
    WHERE grp = 5
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, use_skip_indexes_on_data_read = 1, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- The same query with pruning done during index analysis: read-in-order is kept there because the final
-- selection is genuinely small. Both paths must agree, and they must agree on keeping read-in-order.
SELECT 'analysis_time_pruning_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_data_read_pruning
    WHERE grp = 5
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, use_skip_indexes_on_data_read = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: a leading-wildcard LIKE on a non-indexed column leaves no skip index for either path to use,
-- so no reader is installed, the selection really is the whole table and the guard must still fire.
-- This proves the exemption did not simply switch the guard off for every table that has a skip index.
SELECT 'poor_selectivity_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_data_read_pruning
    WHERE s LIKE '%9'
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, use_skip_indexes_on_data_read = 1, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Correctness: the read-in-order query pruned during the data read returns all matching rows, sorted.
SELECT 'correctness';
SELECT count(), min(id), max(id) FROM (
    SELECT id FROM t_read_in_order_data_read_pruning
    WHERE grp = 5
    ORDER BY id
    SETTINGS enable_parallel_replicas = 0, use_skip_indexes_on_data_read = 1, read_in_order_max_primary_key_ratio = 0.5
);

DROP TABLE t_read_in_order_data_read_pruning;
