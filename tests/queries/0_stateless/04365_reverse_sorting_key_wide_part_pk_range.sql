-- Tags: no-random-merge-tree-settings
-- Regression test for the reverse (DESC) sorting key primary-key range analysis on wide parts.
-- The last mark range was opened towards +inf unconditionally, producing an inverted key range
-- (left > right) for a descending key. That mis-pruned granules (wrong results / data loss) and,
-- for `col IN (set)` predicates, tripped the "Invalid binary search result in MergeTreeSetIndex"
-- assertion (STID 3252-3a4a). See PR #109000 discussion.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_rev;
CREATE TABLE t_rev (id Int64)
ENGINE = MergeTree ORDER BY id DESC
SETTINGS min_rows_for_wide_part = 1, index_granularity_bytes = 0, index_granularity = 8, allow_experimental_reverse_key = 1;
INSERT INTO t_rev SELECT number - 500 FROM numbers(1000);

SELECT 'part_type', part_type FROM system.parts WHERE table = 't_rev' AND database = currentDatabase() AND active;

-- Ranges on a descending single key: index result must match a full scan.
SELECT 'lt0', count() FROM t_rev WHERE id < 0;
SELECT 'ge0', count() FROM t_rev WHERE id >= 0;
SELECT 'between', count() FROM t_rev WHERE id BETWEEN -100 AND 100;
SELECT 'eq_min', count() FROM t_rev WHERE id = -500;
SELECT 'eq_max', count() FROM t_rev WHERE id = 499;

-- The IN(set) path that produced the LOGICAL_ERROR in debug builds.
SELECT 'in_set', count() FROM t_rev WHERE id IN (SELECT number - 500 FROM numbers(50));

DROP TABLE IF EXISTS t_rev2;
CREATE TABLE t_rev2 (id Int64, k UInt32)
ENGINE = MergeTree ORDER BY (id DESC, k DESC)
SETTINGS min_rows_for_wide_part = 1, index_granularity_bytes = 0, index_granularity = 8, allow_experimental_reverse_key = 1;
INSERT INTO t_rev2 SELECT number - 500, number % 7 FROM numbers(1000);

-- Multi-column descending key: range on both key columns and set membership.
SELECT 'multi_range', count() FROM t_rev2 WHERE id BETWEEN -50 AND 50 AND k < 3;
SELECT 'multi_in', count() FROM t_rev2 WHERE id IN (SELECT number - 500 FROM numbers(30));

DROP TABLE t_rev;
DROP TABLE t_rev2;
