-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression test for the `Merge`-table atomicity of the read-in-order PK-selectivity guard.
-- A `Merge` table reads from several underlying `MergeTree` tables. With the guard enabled,
-- the per-child decision can differ: a child with good primary key selectivity would accept
-- read-in-order, while a child with poor selectivity would reject it. Without the all-children
-- dry-run in `ReadFromMerge::requestReadingInOrder`, the accepting children would already be
-- switched to in-order (with their `max_rows_to_read` checks skipped) while the parent falls
-- back to a full sort, leaving mixed semantics across siblings. The parent must instead fall
-- back atomically: either all children read in order, or none do.

DROP TABLE IF EXISTS t_merge_good;
DROP TABLE IF EXISTS t_merge_poor;
DROP TABLE IF EXISTS t_merge_all;

CREATE TABLE t_merge_good (path String) ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

CREATE TABLE t_merge_poor (path String) ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_merge_good;
SYSTEM STOP MERGES t_merge_poor;

-- `t_merge_good`: paths span the whole range `0000`..`9999`, so the range filter `path >= '9900'`
-- selects only the last ~1% of marks -> good selectivity -> this child would accept read-in-order.
INSERT INTO t_merge_good SELECT leftPad(toString(number % 10000), 4, '0') FROM numbers(100000);

-- `t_merge_poor`: every path is already `>= '9900'`, so the same filter selects every mark
-- -> poor selectivity -> this child would reject read-in-order.
INSERT INTO t_merge_poor SELECT concat('99', leftPad(toString(number % 100), 2, '0')) FROM numbers(100000);

CREATE TABLE t_merge_all AS t_merge_good ENGINE = Merge(currentDatabase(), '^t_merge_(good|poor)$');

SET max_threads = 4;

-- Mixed children: one accepts, one rejects -> the parent must fall back to a full sort.
-- A `PartialSortingTransform` proves the parent sorts; the absence of `MergeTreeInOrder` proves
-- no child was left reading in order.
SELECT 'mixed_children_parent_falls_back';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_merge_all
    WHERE path >= '9900'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'mixed_children_no_child_in_order';
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT * FROM t_merge_all
    WHERE path >= '9900'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%MergeTreeInOrder%';

-- Disabling the guard (`read_in_order_max_primary_key_ratio = 1.0`) must let the `Merge` table
-- read all children in order again (no `PartialSortingTransform`).
SELECT 'guard_disabled_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_merge_all
    WHERE path >= '9900'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 1.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Results must be correct (sorted, complete) when the parent falls back.
SELECT 'correctness';
SELECT count(), min(path) >= '9900' AND max(path) <= '9999' FROM (
    SELECT path FROM t_merge_all
    WHERE path >= '9900'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
);

DROP TABLE t_merge_all;
DROP TABLE t_merge_poor;
DROP TABLE t_merge_good;
