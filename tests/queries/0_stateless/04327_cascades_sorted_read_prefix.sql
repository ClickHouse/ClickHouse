-- Tags: no-random-merge-tree-settings
-- `SortedReadImplementation` must not claim the full required sorting when only
-- a prefix of it matches the table's sorting key. With PK `(a)` and required
-- sorting `(a, b)` the read is sorted by `a` only; the optimizer must keep a
-- Sort step (or claim only the prefix), otherwise rows within equal `a` come
-- out in storage order.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_prefix_sorted;

CREATE TABLE t_prefix_sorted (a UInt64, b UInt64) ENGINE = MergeTree() ORDER BY a;

-- Within each `a`, store `b` in descending order so that a missing Sort is visible.
INSERT INTO t_prefix_sorted SELECT intDiv(number, 10), 9 - (number % 10) FROM numbers(50);

SELECT '-- 1. ORDER BY a, b (PK is only a): result must be sorted by (a, b)';
SELECT a, b FROM t_prefix_sorted ORDER BY a, b;

SELECT '-- 2. Same with LIMIT';
SELECT a, b FROM t_prefix_sorted ORDER BY a, b LIMIT 7;

SELECT '-- 3. Baseline without Cascades must produce identical output';
SELECT a, b FROM t_prefix_sorted ORDER BY a, b LIMIT 7
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_prefix_sorted;
