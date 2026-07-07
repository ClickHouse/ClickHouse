-- `parallel_full_sorting_merge` must parallelize a read-in-order merge join too, not run it single-threaded.
-- When both sides read a MergeTree in order of the join key, the pre-join sort is a read-in-order
-- `FinishSorting` (already sorted by the key). Such a side is scattered by the hash of the join keys while
-- preserving order, so each shard only finishes the sort instead of redoing it: the low-cost in-order read
-- is kept and the merge join still runs on all threads. The result must match the `hash` algorithm.

DROP TABLE IF EXISTS pfsmj_rio_left;
DROP TABLE IF EXISTS pfsmj_rio_right;

CREATE TABLE pfsmj_rio_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_rio_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts per side so each read produces multiple in-order streams (the case where the order-preserving
-- scatter must merge the per-shard pieces). Duplicate ids exercise many-to-many matches within a shard.
INSERT INTO pfsmj_rio_left SELECT number % 30000, number FROM numbers(0, 40000);
INSERT INTO pfsmj_rio_left SELECT number % 30000, number FROM numbers(40000, 40000);
INSERT INTO pfsmj_rio_right SELECT number % 20000, number * 2 FROM numbers(0, 50000);
INSERT INTO pfsmj_rio_right SELECT number % 20000, number * 3 FROM numbers(50000, 50000);

-- The join is on `id`, the tables' ORDER BY key, so read-in-order applies. `parallel_full_sorting_merge` must
-- still scatter both sides by the hash of the join key (`ScatterByPartitionTransform`, one per side)...
SELECT 'read_in_order_is_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- ...and it must NOT redo the sort from scratch: a full re-sort would add `MergeSortingTransform`, but the
-- read-in-order side only merges (`MergingSortedTransform`) and finishes the sort, so there is no full sort.
SELECT 'read_in_order_not_resorted', countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- Contrast: plain `full_sorting_merge` on the same query is a single merge join with no scatter.
SELECT 'plain_is_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- Correctness against `hash` for every join kind.
SELECT 'inner',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'left',
    (SELECT (sum(l.a), count()) FROM pfsmj_rio_left AS l LEFT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a), count()) FROM pfsmj_rio_left AS l LEFT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'right',
    (SELECT (sum(r.b), count()) FROM pfsmj_rio_left AS l RIGHT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(r.b), count()) FROM pfsmj_rio_left AS l RIGHT JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full',
    (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full_use_nulls',
    (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', join_use_nulls = 1)
  = (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_rio_left AS l FULL JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash', join_use_nulls = 1);

-- Virtual rows (read_in_order_use_virtual_row) are disabled on a scattered side; results must match with the
-- optimization both off and on, and with per-block virtual rows on.
SELECT 'virtual_row_off',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', read_in_order_use_virtual_row = 0)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'virtual_row_on',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', read_in_order_use_virtual_row = 1)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'virtual_row_per_block_on',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

-- Row-level check: the two result sets must be identical, not just their aggregates.
SELECT 'rows_identical', count() = 0
FROM
(
    (SELECT l.id, l.a, r.b FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
    EXCEPT
    (SELECT l.id, l.a, r.b FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash')
);

DROP TABLE pfsmj_rio_left;
DROP TABLE pfsmj_rio_right;
